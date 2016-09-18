using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    [Serializable]
    public class LogRecord<T>
    {
        public T NewVal { get; set; }
        public GrainVersion Version { get; set; }
    }
}
