using Orleans.CodeGeneration;
using Orleans.Transactions.Abstractions;
using Orleans.Transactions;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Orleans.Transactions.Tests")]

[assembly: GenerateSerializer(typeof(TransactionalExtensionExtensions.TransactionalResourceExtensionWrapper))]
[assembly: GenerateSerializer(typeof(CommitRecord))]
[assembly: GenerateSerializer(typeof(TransactionsConfiguration))]
[assembly: GenerateSerializer(typeof(LogRecord<>))]
[assembly: GenerateSerializer(typeof(TransactionalStateRecord<>))]
