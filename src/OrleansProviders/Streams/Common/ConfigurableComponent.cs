/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

namespace Orleans.Providers.Streams.Common
{
    public interface IComponentConfig
    {
    }

    public interface IConfigurableComponent
    {
        void Configure(ComponentFactory factory, IComponentConfig config);
    }

    public class ComponentFactory
    {
        private readonly Factory<string, IConfigurableComponent> _factory = new Factory<string, IConfigurableComponent>();

        public void Register<T>(string componentId) where T : IConfigurableComponent, new()
        {
            _factory.Register<T>(componentId);
        }

        public T Create<T>(string componentId, IComponentConfig config)
        {
            IConfigurableComponent cpnt = _factory.Create(componentId);
            if (cpnt == null)
                return default(T);

            cpnt.Configure(this, config);

            return (T)cpnt;
        }
    }
}
