using System.Collections.Generic;
using System.Net;

namespace Pizza
{
    public class ProxyManager
    {
        private readonly List<WebProxy> _proxies = new List<WebProxy>();

        private int _index;

        private readonly object _proxyLock = new object();

        public int Count;
        public ProxyManager(string[] proxyList)
        {
            foreach (string line in proxyList)
            {
                WebProxy proxy = new WebProxy("http://" + line, true);

                _proxies.Add(proxy);
            }

            Count = _proxies.Count;
        }

        public WebProxy Get()
        {
            lock (_proxyLock)
            {
                WebProxy proxy = _proxies[_index++];
                if (_index == _proxies.Count) _index = 0;
                return proxy;
            }
        }
    }
}
