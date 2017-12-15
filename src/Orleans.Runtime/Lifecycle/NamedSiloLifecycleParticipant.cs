using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Runtime
{
    public class KeyedSiloLifecycleParticipant<TKey, TService> : IKeyedService<TKey, TService>, ILifecycleParticipant<ISiloLifecycle>
        where TService : class
    {
        private readonly Func<IServiceProvider, TKey, TService> instanceFactory;
        private readonly IServiceProvider services;
        private readonly TKey key;
        private TService service;

        public KeyedSiloLifecycleParticipant(IServiceProvider services, TKey key, Func<IServiceProvider, TKey, TService> instanceFactory)
        {
            this.services = services;
            this.key = key;
            this.instanceFactory = instanceFactory;
        }

        public TService GetService(IServiceProvider services)
        {
            return this.service ?? (this.service = instanceFactory(this.services, this.key));
        }

        public bool Equals(TKey other)
        {
            return Equals(key, other);
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            (this.GetService(this.services) as ILifecycleParticipant<ISiloLifecycle>)?.Participate(lifecycle);
        }
    }

    public class KeyedSiloLifecycleParticipant<TKey, TService, TInstance> : KeyedSiloLifecycleParticipant<TKey, TService>
    where TInstance : TService
    where TService : class
    {
        public KeyedSiloLifecycleParticipant(IServiceProvider services, TKey key)
            : base(services, key, (sp,k) => sp.GetRequiredService<TInstance>())
        {
        }
    }

    public class KeyedSiloLifecycleParticipantCollection<TKey, TService> : KeyedServiceCollection<TKey, TService>, ILifecycleParticipant<ISiloLifecycle>
        where TService : class
    {
        private readonly IServiceProvider services;

        public KeyedSiloLifecycleParticipantCollection(IServiceProvider services)
        {
            this.services = services;
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            foreach(IKeyedService<TKey, TService> keyedService in this.services.GetServices<IKeyedService<TKey, TService>>())
            {
                (keyedService as ILifecycleParticipant<ISiloLifecycle>)?.Participate(lifecycle);
            }
        }
    }

    public static class KeyedSiloLifecycleParticipantExtensions
    {
        /// <summary>
        /// Register a keyed Silo lifecycle participant
        /// </summary>
        public static void AddKeyedSiloLifecycleParticipant<TKey, TService, TInstance>(this IServiceCollection collection, TKey key)
            where TInstance : class, TService
            where TService : class
        {
            collection.TryAddSingleton<TInstance>();
            collection.AddSingleton<IKeyedService<TKey, TService>>(sp => new KeyedSiloLifecycleParticipant<TKey, TService, TInstance>(sp, key));
        }

        /// <summary>
        /// Register a keyed Silo lifecycle participant
        /// </summary>
        public static void AddKeyedSiloLifecycleParticipant<TKey, TService>(this IServiceCollection collection, TKey key, Func<IServiceProvider,TService> serviceFactory)
            where TService : class
        {
            collection.AddSingleton<IKeyedService<TKey, TService>>(sp => new KeyedSiloLifecycleParticipant<TKey, TService>(sp, key, (s,k) => serviceFactory(s)));
        }

        /// <summary>
        /// Register a keyed Silo lifecycle participant
        /// </summary>
        public static void AddKeyedSiloLifecycleParticipant<TKey, TService>(this IServiceCollection collection, TKey key, Func<IServiceProvider, TKey, TService> serviceFactory)
            where TService : class
        {
            collection.AddSingleton<IKeyedService<TKey, TService>>(sp => new KeyedSiloLifecycleParticipant<TKey, TService>(sp, key, serviceFactory));
        }

        /// <summary>
        /// Register a named Silo lifecycle participant
        /// </summary>
        public static void AddNamedSiloLifecycleParticipant<TService, TInstance>(this IServiceCollection collection, string name)
            where TInstance : class, TService
            where TService : class
        {
            collection.AddKeyedSiloLifecycleParticipant<string, TService, TInstance>(name);
        }


        /// <summary>
        /// Register a named Silo lifecycle participant
        /// </summary>
        public static void AddNamedSiloLifecycleParticipant<TService>(this IServiceCollection collection, string name, Func<IServiceProvider, TService> serviceFactory)
            where TService : class
        {
            collection.AddKeyedSiloLifecycleParticipant<string, TService>(name, (sp,n) => serviceFactory(sp));
        }

        /// <summary>
        /// Register a named Silo lifecycle participant
        /// </summary>
        public static void AddNamedSiloLifecycleParticipant<TService>(this IServiceCollection collection, string name, Func<IServiceProvider, string, TService> serviceFactory)
            where TService : class
        {
            collection.AddKeyedSiloLifecycleParticipant<string, TService>(name, serviceFactory);
        }
    }
}
