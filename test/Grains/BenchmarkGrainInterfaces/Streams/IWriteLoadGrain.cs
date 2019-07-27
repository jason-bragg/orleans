using System;
using System.Threading.Tasks;
using Orleans;

namespace BenchmarkGrainInterfaces.Streams
{
    public class Report
    {
        public long Succeeded { get; set; }
        public long Failed { get; set; }
        public TimeSpan Elapsed { get; set; }
    }

    public interface IWriteLoadGrain : IGrainWithGuidKey
    {
        Task Generate(int run, int conncurrent);
        Task<Report> TryGetReport();
    }
}
