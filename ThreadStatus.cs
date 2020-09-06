using System;
using System.Collections.Generic;

namespace Pizza
{
    class ThreadStatus<T>
    {
        static Dictionary<T, bool> threadStatus = new Dictionary<T, bool>();

        public static int threadCount;

        static object Lock = new object();

        public ThreadStatus()
        {
            threadCount = 0;
        }

        public void AddThread(T index, bool value = false)
        {
            lock (Lock)
            {
                threadStatus.Add(index, value);
                threadCount++;
            }
        }
        public ThreadStatus(T[] threadIndexes, bool defaultValue = false)
        {
            lock (Lock)
            {
                threadCount = threadIndexes.Length;

                foreach(T index in threadIndexes)
                {
                    AddThread(index, defaultValue); 
                }
            }
        }

        public void SetAvailableStatus(T index, bool value)
        {
            lock (Lock)
            {
                threadStatus[index] = value;
            }
        }

        public bool AllThreadsAvailable()
        {
            lock (Lock)
            {
                foreach (var thread in threadStatus)
                {
                    if (thread.Value == false) return false;
                }
                return true;
            }
        }

        internal bool AllThreadsBusy()
        {
            lock (Lock)
            {
                foreach (var thread in threadStatus)
                {
                    if (thread.Value == true) return false;
                }
                return true;
            }
        }
    }
}

