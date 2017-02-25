﻿using System;

namespace Orleans.Transactions
{
    [Serializable]
    public class LogRecord<T>
    {
        public T NewVal { get; set; }
        public GrainVersion Version { get; set; }
    }
}
