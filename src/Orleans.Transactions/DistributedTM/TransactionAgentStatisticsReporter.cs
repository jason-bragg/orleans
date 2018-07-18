﻿using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;

namespace Orleans.Transactions
{
    public class TransactionAgentStatisticsReporter : ILifecycleParticipant<ISiloLifecycle>
    {
        private const string TransactionsStartedTotalMetric = "TransactionAgent.TransactionsStarted_Total";
        private const string TransactionsStartedPerSecondMetric = "TransactionAgent.TransactionsStarted_PerSecond";

        private const string SuccessfulTransactionsTotalMetric = "TransactionAgent.SuccessfulTransactions_Total";
        private const string SuccessfulTransactionsPerSecondMetric = "TransactionAgent.SuccessfulTransactions_PerSecond";

        private const string FailedTransactionsTotalMetric = "TransactionAgent.FailedTransactions_Total";
        private const string FailedTransactionsPerSecondMetric = "TransactionAgent.FailedTransactions_PerSecond";

        private const string ThrottledTransactionsTotalMetric = "TransactionAgent.ThrottledTransactions_Total";
        private const string ThrottledTransactionsPerSecondMetric = "TransactionAgent.ThrottledTransactions_PerSecond";

        private readonly ITransactionAgentStatistics statistics;
        private readonly ITelemetryProducer telemetryProducer;
        private readonly StatisticsOptions statisticsOptions;

        private TransactionAgentStatistics lastReported;
        private DateTime lastReportTime;
        private IDisposable timer;

        public TransactionAgentStatisticsReporter(ITransactionAgentStatistics statistics, ITelemetryProducer telemetryProducer, IOptions<StatisticsOptions> options)
        {
            this.statistics = statistics ?? throw new ArgumentNullException(nameof(statistics));
            this.telemetryProducer = telemetryProducer ?? throw new ArgumentNullException(nameof(statistics));
            this.statisticsOptions = options.Value;
            this.lastReported = TransactionAgentStatistics.Create(statistics);
            this.lastReportTime = DateTime.UtcNow;
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe<TransactionAgentStatisticsReporter>(ServiceLifecycleStage.Active, OnStart, OnStop);
        }

        private Task OnStart(CancellationToken tc)
        {
            this.timer = new Timer(ReportMetrics, null, this.statisticsOptions.PerfCountersWriteInterval, this.statisticsOptions.PerfCountersWriteInterval);
            return Task.CompletedTask;
        }

        private Task OnStop(CancellationToken tc)
        {
            this.timer?.Dispose();
            this.timer = null;
            return Task.CompletedTask;
        }


        private void ReportMetrics(object ignore)
        {
            var now = DateTime.UtcNow;
            var currentReported = TransactionAgentStatistics.Create(statistics);

            this.telemetryProducer.TrackMetric(TransactionsStartedTotalMetric, currentReported.TransactionsStarted);
            this.telemetryProducer.TrackMetric(TransactionsStartedPerSecondMetric, PerSecond(this.lastReported.TransactionsStarted, currentReported.TransactionsStarted, now - lastReportTime));

            this.telemetryProducer.TrackMetric(SuccessfulTransactionsTotalMetric, currentReported.TransactionsSucceeded);
            this.telemetryProducer.TrackMetric(SuccessfulTransactionsPerSecondMetric, PerSecond(this.lastReported.TransactionsSucceeded, currentReported.TransactionsSucceeded, now - lastReportTime));

            this.telemetryProducer.TrackMetric(FailedTransactionsTotalMetric, currentReported.TransactionsFailed);
            this.telemetryProducer.TrackMetric(FailedTransactionsPerSecondMetric, PerSecond(this.lastReported.TransactionsFailed, currentReported.TransactionsFailed, now - lastReportTime));

            this.telemetryProducer.TrackMetric(ThrottledTransactionsTotalMetric, currentReported.TransactionsThrottled);
            this.telemetryProducer.TrackMetric(ThrottledTransactionsPerSecondMetric, PerSecond(this.lastReported.TransactionsThrottled, currentReported.TransactionsThrottled, now - lastReportTime));

            this.lastReportTime = now;
            this.lastReported = currentReported;
        }

        private long PerSecond(long start, long end, TimeSpan time)
        {
            return ((end - start) * 1000) / Math.Max(1,(long)time.TotalMilliseconds);
        }
    }
}
