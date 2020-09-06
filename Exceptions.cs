using System;

namespace Pizza
{
    public class NullResponseException : Exception
    {
        public NullResponseException()
        {
        }

        public NullResponseException(string message)
            : base(message)
        {
        }

        public NullResponseException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}

