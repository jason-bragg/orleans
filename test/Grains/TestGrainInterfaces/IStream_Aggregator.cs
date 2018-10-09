using Orleans;
using System;
using System.Threading.Tasks;

namespace UnitTests.GrainInterfaces
{
    public class Stream_AggregationOptions
    {
        public string StreamProviderName { get; set; }

        public const string Int_Aggregator_StreamNamespace = "Int_Aggregator";
        public const string Int_Aggregation_StreamNamespace = "Int_Aggregation";
        public const string String_Aggregator_StreamNamespace = "String_Aggregator";
        public const string String_Aggregation_StreamNamespace = "String_Aggregation";
    }


    public interface IIntStream_Aggregator : IGrainWithGuidKey
    {
        Task<int> GetNumberConsumed();
    }

    public interface IStringStream_Aggregator : IGrainWithGuidKey
    {
        Task<int> GetNumberConsumed();
    }

    public interface IStream_Aggregation : IGrainWithGuidKey
    {
        Task<Tuple<int, int>> GetNumberConsumed();
    }
}
