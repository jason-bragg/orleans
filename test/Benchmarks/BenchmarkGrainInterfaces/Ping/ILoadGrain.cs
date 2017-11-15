using System;
using System.Threading.Tasks;
using Orleans;

namespace BenchmarkGrainInterfaces.Ping
{
    public class Report
    {
        public int Succeeded { get; set; }
        public int Failed { get; set; }
        public TimeSpan Elapsed { get; set; }
    }

    public interface ILoadGrain : IGrainWithGuidKey
    {
        Task Generate(int run, int conncurrent, TimeSpan duration);
        Task<Report> TryGetReport();
    }
}
