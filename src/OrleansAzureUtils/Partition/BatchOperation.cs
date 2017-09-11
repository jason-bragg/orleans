using System;
using System.Collections.Generic;
using Orleans.AzureUtils.Abstractions;
using Microsoft.WindowsAzure.Storage.Table;

namespace Orleans.AzureUtils
{
    /// <summary>
    /// A batch operation embodies a set of modifications to be made to a partition.
    /// - Batch must be committed to the partition for operations to be applied.
    /// - Storage operations in a batch will either all succeed or all fail.
    /// - No more than 100 operations are supported per batch.
    /// </summary>
    public class BatchOperation : IBatchOperation
    {
        private readonly List<Tuple<StorageOperation, ITableEntity>> operations = new List<Tuple<StorageOperation, ITableEntity>>();

        /// <summary>
        /// Creates a new instance of <see cref="BatchOperation"/> 
        /// </summary>
        public BatchOperation()
        {
        }

        public IEnumerable<Tuple<StorageOperation, ITableEntity>> Operations
        {
            get { return this.operations; }
        }

        /// <summary>
        /// Takes a table entity and inserts it
        /// This can fail if the entity already exists in storage
        /// </summary>
        /// <param name="entity">Table Entity</param>
        public void Insert<T>(T entity) where T : class, ITableEntity, new()
        {
            this.operations.Add(Tuple.Create(StorageOperation.Insert, (ITableEntity)entity));
        }

        /// <summary>
        /// Takes a table entity and sets the operation to InsertOrReplace. This will overwrite whatever value currently exists ignoring etag
        /// This can be useful in scenarios where you expect identical data to be written more than once
        /// </summary>
        /// <param name="entity">Table entity</param>
        public void InsertOrReplace<T>(T entity) where T : class, ITableEntity, new()
        {
            this.operations.Add(Tuple.Create(StorageOperation.InsertOrReplace, (ITableEntity)entity));
        }

        /// <summary>
        /// Takes a table entity and inserts/adds it based on whether it has a etag. 
        /// The correct usage pattern for this method depends on the user making a query for this entity before using it (or else it will always attempt an insert)
        /// </summary>
        /// <param name="entity">Table Entity</param>
        public void Upsert<T>(T entity) where T : class, ITableEntity, new()
        {
            this.operations.Add(string.IsNullOrEmpty(entity.ETag)
                    ? Tuple.Create(StorageOperation.Insert, (ITableEntity)entity)
                    : Tuple.Create(StorageOperation.Update, (ITableEntity)entity));
        }

        /// <summary>
        /// Delete the given entity.
        /// </summary>
        public void Delete(ITableEntity entity)
        {
            this.operations.Add(Tuple.Create(StorageOperation.Delete, entity));
        }
    }
}
