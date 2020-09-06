using System.Collections.Generic;

namespace Pizza
{
    
    class QueueManager<T>
    {
        private LinkedList<T> _queue;
        private object Lock = new object();

        public QueueManager()
        {
            _queue = new LinkedList<T>();
        }

        public QueueManager(string name)
        {
            _queue = new LinkedList<T>();
        }

        public QueueManager(T[] initValues)
        {
            _queue = new LinkedList<T>(initValues);
        }
        
        public void RefreshValues(T[] values)
        {
            lock (Lock)
            {
                _queue = new LinkedList<T>(values);
            }
        }

        public T Take()
        {
            lock (Lock)
            {
                while (_queue.Count == 0) ; // wait until something is added
                T item = _queue.First.Value;
                _queue.RemoveFirst();
                return item;
            }
        }

        public void Add(T item)
        {
            lock (Lock)
            {
                _queue.AddFirst(item);
            }
        }

        public void AddLast(T item)
        {
            lock (Lock)
            {
                _queue.AddLast(item);
            }
        }

        public int Count()
        {
            lock (Lock)
            {
                return _queue.Count;
            }
        }

        public void AcquireLock()
        {
            System.Threading.Monitor.Enter(Lock);
        }

        public void ReleaseLock()
        {
            System.Threading.Monitor.Exit(Lock);
        }
    }
}
