// #define bruteForcePhash
/* phash brute force method. doesn't require elasticsearch
 * draws phashes from ph_distinct table and waits until item is added in database */

using System;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using HtmlAgilityPack;
using Microsoft.CSharp.RuntimeBinder;
using Newtonsoft.Json;
using Npgsql;
using Shipwreck.Phash;
using Shipwreck.Phash.Bitmaps;

public static class Extensions
{
    public static byte[] ToByteArray(this string hex)
    {
        hex = hex.Substring(2, hex.Length - 2); // remove "0x"
        var bytes = new byte[hex.Length / 2];

        for (var i = 0; i < hex.Length; i += 2)
            bytes[i / 2] = Convert.ToByte(hex.Substring(i, 2), 16);

        return bytes;
    }

    public static string ByteArrayToString(this byte[] bytes)
    {
        return "0x" + BitConverter.ToString(bytes).Replace("-", "");
    }
}

namespace Pizza
{
    public static class Program
    {
        private static ProxyManager _proxy = new ProxyManager(File.ReadAllLines("Proxy.txt"));

        private static readonly QueueManager<string> Queue =
            new QueueManager<string>(File.ReadAllLines("Listings.txt"));

        private static Dictionary<string, byte[]> _avatars = new Dictionary<string, byte[]>();
        private static bool _startScan;
        private static int _count;

        private static readonly object SqlLock = new object();
        private static readonly object CountLock;

        private static readonly NpgsqlConnection PizzaDb =
            new NpgsqlConnection(
                @"server=localhost;port=5432;database=pizza;userid=postgres;password='diT59%w1gQK$ptI1bvGP';timeout=0");

        private static readonly NpgsqlConnection SteamTracker =
            new NpgsqlConnection(
                @"server=localhost;port=5432;database=steamtracker;userid=postgres;password='diT59%w1gQK$ptI1bvGP';timeout=0");

        private static int _callsPerMinute;
        private static int _deadCallsPerMinute;

        private static readonly ThreadStatus<int> ThreadStatus = new ThreadStatus<int>(); // manages thread's "busyness"

        private static readonly byte[] DefaultAvatar =
            "0x9E0032FFD69483B69AA1A1A6B0948BAAA09EA3A0A49892A29F9BA19EA19995A4A49DA19C9D989AA2"
                .ToByteArray(); // to filter out default avatars, it's used by lot of users

        private const long DistinctPhashAmount = 74390985; // hardcoded value, oops

        static Program()
        {
            CountLock = new object();
        }

        private static void Main(string[] args)
        {
            PizzaDb.Open();
#if (bruteForcePhash)
            SteamTracker.Open();
#endif
            _callsPerMinute = 0;
            _deadCallsPerMinute = 0;

            
            int threadCount = args.Length > 0 ? int.Parse(args[0]) : 500;
            
            for (var i = 0; i < threadCount; i++)
                new Thread(Scan).Start();

#if (bruteForcePhash)
            for (int i = 0; i < threadCount; i++)
            {
                int tempI = i; // memory is shared so before thread starts, loop iterates and value of i changes
                int tempOffset = offset;

                threadStatus.AddThread(tempI, false);
                // start thread with different offsets
                new Thread(() => ComparePhashes(tempI, tempOffset, limit))
                    .Start();

                offset += limit;
            }

            new Thread(IdentifyAvatars)
                .Start();
#endif
            while (true)
            {
                Console.WriteLine(
                    $"{_callsPerMinute - _deadCallsPerMinute} / {_callsPerMinute + _deadCallsPerMinute} call /m");
                _callsPerMinute = 0;
                _deadCallsPerMinute = 0;

                Thread.Sleep(60000);
                if (File.Exists("refresh")) // terrible way to interact with program
                {
                    File.Delete("refresh");
                    Queue.RefreshValues(File.ReadAllLines("Listings.txt"));
                    _proxy = new ProxyManager(File.ReadAllLines("Proxy.txt"));
                    Console.WriteLine("Refreshed proxy & listings");
                }
            }
        }

        private static DataTable Query(string command, NpgsqlConnection database)
        {
            lock (SqlLock)
            {
                var sql = new NpgsqlCommand(command, database);
                return Query(sql);
            }
        }

        private static DataTable Query(NpgsqlCommand command)
        {
            var dt = new DataTable();
            command.CommandTimeout = 0;
            lock (SqlLock)
            {
                try
                {
                    var reader = command.ExecuteReader();
                    dt.Load(reader);
                    reader.Close();
                }
                catch (InvalidOperationException) // for some reason, postgres randomly drops all connections
                {
                    Console.WriteLine("Exiting Environment");
                    Environment.Exit(1); // run.sh script will restart app if exit code is not 0
                }
            }

            return dt;
        }

        private static NpgsqlDataReader ReaderQuery(string command, NpgsqlConnection database)
        {
            lock (SqlLock)
            {
                var sql = new NpgsqlCommand(command, database);

                sql.CommandTimeout = 0;
                var reader = sql.ExecuteReader();
                return reader;
            }
        }

        private static bool AlreadyInDatabase(string listing, string marketAvatar, string table)
        {
            var sql =
                new NpgsqlCommand($"SELECT id FROM {table} WHERE item=@item AND marketAvatar=@marketAvatar LIMIT 1",
                    PizzaDb);

            sql.Parameters.AddWithValue("item", listing);
            sql.Parameters.AddWithValue("marketAvatar", marketAvatar);
            return Query(sql).Rows.Count > 0;
        }


        private static void Scan()
        {
            while (true)
            {
                var proxy = _proxy.Get();
                var url = GetListing();
                
                try
                {
                    if (HandleListing(url, proxy))
                        // if parsing listing was successful, add it to LAST place in queue
                        Queue.AddLast(url);
                    else
                        // if parsing listing was NOT successful, re-add it to FIRST place in queue so other proxy will parse it
                        // if it keeps getting added to last place it might never get scanned
                        Queue.Add(url);
                }
                catch (Exception e)
                {
                    if (!(e is NullResponseException || e is JsonReaderException)) 
                        throw;
                    
                    Queue.Add(url);
                    _deadCallsPerMinute++;
                }
            }
        }

        // <summary>
        // fetches html, parses and adds it to database
        // </summary>
        // <returns>whether listing was added to database</returns>
        private static bool HandleListing(string url, WebProxy proxy)
        {
            try
            {
                // TODO: make GetHttpResponse function
                var client = new HttpClient(new HttpClientHandler
                {
                    Proxy = proxy,
                    UseProxy = true,
                    AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
                });
                client.BaseAddress = new Uri(url + "/render");
                client.Timeout = TimeSpan.FromSeconds(5);

                var httpResponse =
                    client.GetAsync("?query=&start=0&count=100&country=GE&language=english&currency=1").Result;
                var result = httpResponse.Content.ReadAsStringAsync().Result;
                _callsPerMinute++;

                // if null was returned, steam temporarily banned ip
                if (result == "null")
                    throw new NullResponseException();

                dynamic response = JsonConvert.DeserializeObject(result);

                if (response["listinginfo"] == null) throw new NullResponseException();
                if (response["listinginfo"].Count == 0) return true;

                // parse AND add listing to database
                ParseHtmlAndAdd((string) response["results_html"], url);

                return true;
            }
            catch (AggregateException)
            {
                return false;
            }
            catch (WebException)
            {
                return false;
            }
            catch (RuntimeBinderException)
            {
                throw new NullResponseException();
            }
        }

        // <summary>
        // parses listing HTML page and adds it to database
        // </summary> 
        private static void ParseHtmlAndAdd(string html, string listingLink)
        {
            var document = new HtmlDocument();
            document.LoadHtml(html);
            var collection =
                document.DocumentNode.SelectNodes("//div[@class='market_listing_right_cell market_listing_seller']");
            var item = document.DocumentNode.SelectSingleNode("//div[@class='market_listing_item_name_block']")
                .SelectSingleNode("span").InnerHtml;
            foreach (var div in collection)
            {
                var img = div.SelectSingleNode("span").SelectSingleNode("span").SelectSingleNode("img");


                // separate image hash from full link
                var url = img.Attributes["src"].Value;
                var hashLength = url.Contains("cloudflare") ? 178 : 168;

                var index = url.IndexOf("/image/") + "/image/".Length;
                var marketAvatar = url.Substring(index, hashLength);

                var price = document.DocumentNode.SelectNodes("div")[1].SelectNodes("div")[1].SelectNodes("div")[1]
                    .SelectSingleNode("span").SelectSingleNode("span").InnerHtml;
                int priceInt;
                
                try
                {
                    priceInt = int.Parse(Regex.Match(price, @"\d+").Value, NumberFormatInfo.InvariantInfo);
                    // separate price tag from whitespace
                    price = price.Substring(price.IndexOf("$"));
                    price = price.Substring(0, price.IndexOf("USD") - 1);
                }
                catch // "SOLD!" instead of price tag
                {
                    continue;
                }

                var table = priceInt > 200 ? "listings" : "litelistings";
                if (AlreadyInDatabase(listingLink, marketAvatar, table)) continue;

                var shortName = item;
                shortName = shortName.Replace(" | ", " ");
                shortName = shortName.Replace("StatTrak™", "ST");
                shortName = shortName.Replace("Factory New", "FN");
                shortName = shortName.Replace("Minimal Wear", "MW");
                shortName = shortName.Replace("Field-Tested ", "FT");
                shortName = shortName.Replace("Well-Worn", "WW");
                shortName = shortName.Replace("Battle-Scarred", "BS");

                AddListing(listingLink, marketAvatar, price, shortName, table);
            }
        }

        private static void AddListing(string url, string marketAvatar, string price, string shortName, string table)
        {
            var cmd =
                new NpgsqlCommand(
                    $"INSERT INTO {table}(item, marketAvatar, price, time) VALUES(@url, @marketAvatar, @price, @time)",
                    PizzaDb);
            cmd.Parameters.AddWithValue("url", url);
            cmd.Parameters.AddWithValue("marketAvatar", marketAvatar);
            cmd.Parameters.AddWithValue("price", price);
            cmd.Parameters.AddWithValue("time", DateTime.UtcNow.Add(new TimeSpan(0, 4, 0, 0)));

            Console.WriteLine($"{table} | {price} | {shortName}");
            Query(cmd);
        }

        // runs in background and feeds avatars to other threads to be identified
        private static void IdentifyAvatars()
        {
            var t = DateTime.Now;
            while (!ThreadStatus.AllThreadsAvailable()) 
                Thread.Sleep(10); // wait until setup finishes saving phash bytes in memory 
            Console.WriteLine($"Setup finished ({DateTime.Now - t})");

            while (true)
            {
                var length = 0;
                DataTable avatarsToIdentify;

                do
                {
                    // combine unidentified avatars of both tables
                    avatarsToIdentify = Query(
                        "SELECT avatar, marketAvatar FROM listings WHERE scanned = false UNION ALL "
                        + "SELECT avatar, marketAvatar FROM litelistings WHERE scanned = false", PizzaDb);
                    length = avatarsToIdentify.Rows.Count;
                } while (length == 0);

                if (length > 10) 
                {
                    Query("UPDATE listings SET scanned=true where scanned=false", PizzaDb);
                    Query("UPDATE litelistings SET scanned=true where scanned=false", PizzaDb);
                    continue;
                }

                Console.WriteLine("Collecting avatars");

                // i know parallel foreach isn't best here
                Parallel.ForEach(avatarsToIdentify.AsEnumerable(), row =>
                {
                    var marketAvatar = row["marketAvatar"].ToString();
                    if (_avatars.ContainsKey(marketAvatar)) return;
                    try
                    {
                        var img =
                            GetImage($"https://steamcommunity-a.akamaihd.net/market/image/{marketAvatar}/avatar.jpg");
                        var marketAvatarPhash = ComputePhash(img).ToByteArray();

                        if (ImagePhash.GetCrossCorrelation(marketAvatarPhash, DefaultAvatar) > 0.99f)
                        {
                            // if avatar matches default steam avatar, don't waste time on it
                            Query(
                                $"UPDATE listings SET scanned = true, avatar='fe/fef49e7fa7e1997310d705b2a6158ff8dc1cdfeb' WHERE marketAvatar = '{marketAvatar}'",
                                PizzaDb);
                            Query(
                                $"UPDATE litelistings SET scanned = true, avatar='fe/fef49e7fa7e1997310d705b2a6158ff8dc1cdfeb' WHERE marketAvatar = '{marketAvatar}'",
                                PizzaDb);
                            return;
                        }

                        _avatars.Add(marketAvatar, marketAvatarPhash);
                        //Console.WriteLine($"added {marketAvatar}");
                    }
                    catch
                    {
                    } // dark avatars like default fail phashing on linux at Shipwreck.Phash.Bitmaps.BitmapExtensions.ToLuminanceImage(Bitmap bitmap) 
                });


                // no avatars to identify
                if (_avatars.Count == 0)
                {
                    Thread.Sleep(1000);
                    continue;
                }

                Console.WriteLine($"{_avatars.Count} Avatars to identify");
                t = DateTime.Now;

                _count = 0;
                _startScan = true;
                /* setting startScan to true starts loop on threads
                 * wait until all threads start iterating */
                while (!ThreadStatus.AllThreadsBusy())
                    Thread.Sleep(10);
                _startScan = false;

                while (!ThreadStatus.AllThreadsAvailable()) // wait until all threads finish comparing phashes
                {
                    // print percentage
                    Console.WriteLine(
                        $"{Math.Round((float) _count / DistinctPhashAmount * 100, 2)}% ({DateTime.Now - t})");
                    Thread.Sleep(1000);
                }

                foreach (var pair in _avatars)
                {
                    /* some phashes might not be identified at all, and some might be found multiple times across whole database
                     *  we can't be sure if there's more so we just wait until we iterate through all */
                    var marketAvatar = pair.Key;

                    Query($"UPDATE listings SET scanned=true where marketAvatar='{marketAvatar}'", PizzaDb);
                    Query($"UPDATE litelistings SET scanned=true where marketAvatar='{marketAvatar}'", PizzaDb);
                }

                _avatars = new Dictionary<string, byte[]>();
            }
        }

        /* each thread uses different offset to store phashes from database
         * after that they go into loop waiting for startScan to become true
         * then they compare phashes they stored to avatars to identify */
        private static void ComparePhashes(int threadN, long offset, long limit)
        {
            var t = DateTime.Now;
            var phashes = new List<byte[]>();

            // create separate instance of database connection for each thread
            var connectionClone = new NpgsqlConnection(
                @"server=localhost;port=5432;database=steamtracker;userid=postgres;password='diT59%w1gQK$ptI1bvGP';timeout=0");
            connectionClone.Open();

            var reader = ReaderQuery($"SELECT phash FROM ph_distinct ORDER BY id ASC OFFSET {offset} LIMIT {limit}",
                connectionClone);
            Console.WriteLine($"Thread {threadN} query done, reading ({DateTime.Now - t})");
            while (reader.Read())
            {
                var phashStr = reader.GetString(0);
                phashes.Add(phashStr.ToByteArray());
            }

            connectionClone.Close();
            //Serializer.Save(byteDumpPath, phashes);
            //Console.WriteLine("Serialized offset " + offset);

            while (true)
            {
                ThreadStatus.SetAvailableStatus(threadN, true);

                while (!_startScan)
                    Thread.Sleep(100);

                ThreadStatus.SetAvailableStatus(threadN, false);

                foreach (var phash in phashes)
                {
                    foreach (var (marketAvatar, marketAvatarPhash) in _avatars)
                    {
                        if (!DifferenceBetweenBytes(phash, marketAvatarPhash, 10))
                            continue; // working on different approach, for now this will improve performance

                        var match = ImagePhash.GetCrossCorrelation(phash, marketAvatarPhash);
                        if (!(match > 0.99f)) continue;
                        try
                        {
                            var phashString = phash.ByteArrayToString();
                            var sql = new NpgsqlCommand($"SELECT avatar FROM users WHERE phash = '{phashString}'",
                                SteamTracker);
                            var avatar = Query(sql).Rows[0]["avatar"].ToString();
                            Console.WriteLine($"Thread {threadN} identified {avatar}");

                            Query(
                                $"UPDATE listings SET avatar=avatar || '{avatar}' || ';' WHERE marketAvatar='{marketAvatar}'",
                                PizzaDb); // append avatar to avatar, delimiter ";"
                            Query(
                                $"UPDATE litelistings SET avatar=avatar || '{avatar}' || ';' WHERE marketAvatar='{marketAvatar}'",
                                PizzaDb); // append avatar to avatar, delimiter ";"
                        }
                        catch
                        {
                            Console.WriteLine(phash.ByteArrayToString());
                        }
                    }

                    lock (CountLock)
                    {
                        _count++;
                    }
                }
            }
        }

        /* compares byte arrays. check first couple bytes to save cpu
         * instead of trying to find phash percentage for every phash*/
        private static bool DifferenceBetweenBytes(byte[] hash1, byte[] hash2, int max)
        {
            if (hash1 == null || hash2 == null) return false;

            for (var i = 0; i < max; i++)
            {
                int val1 = hash1[i];
                int val2 = hash2[i];
                if (val1 != val2 && val1 + 1 != val2 && val1 - 1 != val2) return false;
            }

            return true;
        }

        private static string GetListing()
        {
            while (Queue.Count() == 0)
                Thread.Sleep(10);
            return Queue.Take();
        }

        private static string ComputePhash(Bitmap img)
        {
            return ImagePhash.ComputeDigest(img.ToLuminanceImage())
                .ToString();
        }

        private static Bitmap GetImage(string url)
        {
            var wc = new WebClient();

            var originalData = wc.DownloadData(url);
            var stream = new MemoryStream(originalData);

            return new Bitmap(stream);
        }
    }
}

/*
CREATE TABLE listings(
    id SERIAL,
	item VARCHAR(255) NOT NULL,
    avatar TEXT NOT NULL DEFAULT '',
	time TIMESTAMP NULL,
	price VARCHAR(15),
	scanned BOOL DEFAULT FALSE,
	details TEXT,
	removed BOOL NOT NULL DEFAULT FALSE,
	marketavatar VARCHAR(168) NOT NULL
    ); 

CREATE INDEX ON listings(id DESC);
CREATE INDEX ON listings(time DESC); 
*/