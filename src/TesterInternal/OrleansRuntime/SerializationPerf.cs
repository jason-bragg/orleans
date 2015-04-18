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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.CodeGeneration;
using Orleans.Runtime;
using Orleans.Serialization;

namespace UnitTests.OrleansRuntime
{
    [TestClass]
    public class SerializationPerf
    {
        [Serializable]
        public class blarg
        {
            public blarg()
            {
                i = 1;
                d = 3;
                s = "bob";
                f = (float)5.5;
            }
            public int i { get; set; }
            public double d { get; set; }
            public string s { get; set; }
            public float f { get; set; }
        }
        [TestMethod, TestCategory("Nightly")]
        public void SpeedRun()
        {
            SerializationManager.InitializeForTesting();
            var args = new object[] { new blarg(), new blarg() };

            for (int k = 0; k < 20; k++)
            {
                Stopwatch sw = Stopwatch.StartNew();
                for (int i = 0; i < 2000; i++)
                {
                    int headerSize;
                    var argsDeepCopy = (object[])SerializationManager.DeepCopy(args);
                    var request = new InvokeMethodRequest(1, 2, argsDeepCopy);
                    var message = RuntimeClient.CreateMessage(request, null, InvokeMethodOptions.None);
                    var buffers = message.Serialize(out headerSize);
                    BufferPool.GlobalPool.Release(buffers);
                }
                Console.WriteLine(sw.ElapsedTicks);
                Console.WriteLine(sw.ElapsedMilliseconds);
            }
            Console.WriteLine("---------------------");

            for (int k = 0; k < 20; k++)
            {
                Stopwatch sw = Stopwatch.StartNew();
                int headerSize;
                for (int i = 0; i < 2000; i++)
                {
                    var request = new InvokeMethodRequest(1, 2, args);
                    var bodyStream = new BinaryTokenStreamWriter();
                    SerializationManager.Serialize(request, bodyStream);
                    // We don't bother to turn this into a byte array and save it in bodyBytes because Serialize only gets called on a message
                    // being sent off-box. In this case, the likelihood of needed to re-serialize is very low, and the cost of capturing the
                    // serialized bytes from the steam -- where they're a list of ArraySegment objects -- into an array of bytes is actually
                    // pretty high (an array allocation plus a bunch of copying).
                    var bodyBytes = bodyStream.ToBytes() as List<ArraySegment<byte>>;

                    var message = RuntimeClient.CreateMessage(request, bodyBytes, InvokeMethodOptions.None);

                    var buffers = message.Serialize(out headerSize);
                    BufferPool.GlobalPool.Release(buffers);
                }
                Console.WriteLine(sw.ElapsedTicks);
                Console.WriteLine(sw.ElapsedMilliseconds);
            }

            /*
            Dictionary<Guid, int> gd = new Dictionary<Guid, int>();
            sw = Stopwatch.StartNew();
            for (int i = 0; i < 3500; i++)
            {
                gd.Add(Guid.NewGuid(), i);
            }
            Console.WriteLine(sw.ElapsedTicks);
            Console.WriteLine(sw.ElapsedMilliseconds);

            sw = Stopwatch.StartNew();
            foreach(Guid key in gd.Keys)
            {
                int o;
                gd.TryGetValue(key, out o);
            }
            Console.WriteLine(sw.ElapsedTicks);
            Console.WriteLine(sw.ElapsedMilliseconds);

            gd = new Dictionary<Guid, int>();
            var k = Guid.NewGuid();
            gd.Add(k,1);
            sw = Stopwatch.StartNew();
            for (int i = 0; i < 3500; i++)
            {
                int o;
                gd.TryGetValue(k, out o);
            }
            Console.WriteLine(sw.ElapsedTicks);
            Console.WriteLine(sw.ElapsedMilliseconds);
            */
        }
    }
}
