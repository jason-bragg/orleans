
using System;
using System.Collections.Generic;

namespace Orleans.Factory
{
    internal interface IFactory<out TType>
        where TType : class
    {
        TType Create();
    }

    internal interface IFactory<in TKey, out TType>
        where TKey : IComparable<TKey>
        where TType : class
    {
        TType Create(TKey key);
    }
}
