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

using Tester.TestStreamProviders.Generator;

namespace Tester.TestStreamProviders.SimpleGeneratorStreamProvider
{
    /// <summary>
    /// Component factory that contains all the stream generators available to the simple generator stream provider
    /// 
    /// Since we don't want to write a stream provider for every possible stream generation senario we want, instead
    ///  we can just write a stream generator component, along with it's configuration class, and register it in this component factory.
    /// The simple generator stream provider can be configured to generate streams using any generator registered in this factory.
    /// </summary>
    public class SimpleGeneratorComponentFactory : ComponentFactory
    {
        public SimpleGeneratorComponentFactory()
        {
            // Register any generators we may want to use.  The generated created at runtime is determined by configuration.
            Register<SimpleGenerator>(SimpleGenerator.TypeId);
        }
    }
}
