using System;

namespace Orleans.Factory
{
    public static class FactoryTypeFactory
    {
        public static Type CreateFactoryInterface(Type ttype)
        {
            Type[] typeArgs = { ttype };
            return typeof(IFactory<>).MakeGenericType(typeArgs);
        }
        public static Type CreateFactoryInterface(Type tkey, Type ttype)
        {
            Type[] typeArgs = { tkey, ttype };
            return typeof(IFactory<,>).MakeGenericType(typeArgs);
        }
    }
}
