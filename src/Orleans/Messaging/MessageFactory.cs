using System;
using System.Collections.Generic;
using Orleans.CodeGeneration;
using Orleans.Transactions;

namespace Orleans.Runtime
{
    internal class MessageFactory
    {
        private readonly Logger logger;

        public MessageFactory()
        {
            this.logger = LogManager.GetLogger(nameof(MessageFactory), LoggerType.Runtime);
        }

        public Message CreateMessage(InvokeMethodRequest request, InvokeMethodOptions options)
        {
            var direction = (options & InvokeMethodOptions.OneWay) != 0 ? Message.Directions.OneWay : Message.Directions.Request;
            var message = new Message
            {
                Category = Message.Categories.Application,
                Direction = direction,
                Id = CorrelationId.GetNext(),
                IsReadOnly = (options & InvokeMethodOptions.ReadOnly) != 0,
                IsUnordered = (options & InvokeMethodOptions.Unordered) != 0,
                IsTransactionRequired = (options & InvokeMethodOptions.TransactionRequiresNew) != 0 || (options & InvokeMethodOptions.TransactionRequired) != 0,
                BodyObject = request
            };

            TransactionInfo transactionInfo = message.IsTransactionRequired ? TransactionContext.GetTransactionInfo() : null;
            if (transactionInfo != null)
            {
                transactionInfo.PendingCalls++;
                message.TransactionInfo = new TransactionInfo(transactionInfo);
            }

            if ((options & InvokeMethodOptions.AlwaysInterleave) != 0)
                message.IsAlwaysInterleave = true;

            message.RequestContextData = RequestContext.Export();
            return message;
        }

        public Message CreateResponseMessage(Message request)
        {
            var response = new Message
            {
                Category = request.Category,
                Direction = Message.Directions.Response,
                Id = request.Id,
                IsReadOnly = request.IsReadOnly,
                IsAlwaysInterleave = request.IsAlwaysInterleave,
                TargetSilo = request.SendingSilo,
                IsTransactionRequired = request.IsTransactionRequired
            };

            if (request.SendingGrain != null)
            {
                response.TargetGrain = request.SendingGrain;
                if (request.SendingActivation != null)
                {
                    response.TargetActivation = request.SendingActivation;
                }
            }

            response.SendingSilo = request.TargetSilo;
            if (request.TargetGrain != null)
            {
                response.SendingGrain = request.TargetGrain;
                if (request.TargetActivation != null)
                {
                    response.SendingActivation = request.TargetActivation;
                }
                else if (request.TargetGrain.IsSystemTarget)
                {
                    response.SendingActivation = ActivationId.GetSystemActivation(request.TargetGrain, request.TargetSilo);
                }
            }

            if (request.DebugContext != null)
            {
                response.DebugContext = request.DebugContext;
            }

            response.CacheInvalidationHeader = request.CacheInvalidationHeader;
            response.Expiration = request.Expiration;

            var contextData = RequestContext.Export();
            if (contextData != null)
            {
                response.RequestContextData = contextData;
            }

            TransactionInfo transactionInfo = response.IsTransactionRequired ? TransactionContext.GetTransactionInfo() : null;
            if (transactionInfo != null)
            {
                transactionInfo.PendingCalls++;
                response.TransactionInfo = new TransactionInfo(transactionInfo);
            }

            return response;
        }

        public Message CreateRejectionResponse(Message request, Message.RejectionTypes type, string info, OrleansException ex = null)
        {
            var response = this.CreateResponseMessage(request);
            response.Result = Message.ResponseTypes.Rejection;
            response.RejectionType = type;
            response.RejectionInfo = info;
            response.BodyObject = ex;
            if (this.logger.IsVerbose) this.logger.Verbose("Creating {0} rejection with info '{1}' for {2} at:" + Environment.NewLine + "{3}", type, info, this, Utils.GetStackTrace());
            return response;
        }
    }
}