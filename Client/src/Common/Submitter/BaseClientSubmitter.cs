// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2024. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Client;
using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.DevelopmentKit.Client.Common.Status;
using ArmoniK.DevelopmentKit.Common;
using ArmoniK.DevelopmentKit.Common.Exceptions;
using ArmoniK.DevelopmentKit.Common.Utils;
using ArmoniK.Utils;

using Google.Protobuf;

using Grpc.Core;
using Grpc.Net.Client;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using CreateSessionRequest = ArmoniK.Api.gRPC.V1.Sessions.CreateSessionRequest;
using FilterField = ArmoniK.Api.gRPC.V1.Results.FilterField;
using Filters = ArmoniK.Api.gRPC.V1.Tasks.Filters;
using FiltersAnd = ArmoniK.Api.gRPC.V1.Results.FiltersAnd;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.DevelopmentKit.Client.Common.Submitter;

/// <summary>
///   Base Object for all Client submitter
///   Need to pass the child object Class Type
/// </summary>
[PublicAPI]
// TODO: This should not be a public API. Public API should be defined in an interface.
public abstract class BaseClientSubmitter<T>
{
  /// <summary>
  ///   The number of chunk to split the payloadsWithDependencies
  /// </summary>
  private readonly int chunkSubmitSize_;

  private readonly int configuration_;

  private readonly Properties properties_;

  /// <summary>
  ///   Base Object for all Client submitter
  /// </summary>
  /// <param name="properties">Properties used to create grpc clients</param>
  /// <param name="loggerFactory">the logger factory to pass for root object</param>
  /// <param name="taskOptions"></param>
  /// <param name="session"></param>
  /// <param name="chunkSubmitSize">The size of chunk to split the list of tasks</param>
  protected BaseClientSubmitter(Properties     properties,
                                ILoggerFactory loggerFactory,
                                TaskOptions    taskOptions,
                                Session?       session,
                                int            chunkSubmitSize = 500)
  {
    LoggerFactory    = loggerFactory;
    TaskOptions      = taskOptions;
    properties_      = properties;
    Logger           = loggerFactory.CreateLogger<T>();
    chunkSubmitSize_ = chunkSubmitSize;

    ChannelPool = ClientServiceConnector.ControlPlaneConnectionPool(properties_,
                                                                    LoggerFactory);

    SessionId = session ?? CreateSessionAsync(new[]
                                              {
                                                TaskOptions.PartitionId,
                                              })
                  .WaitSync();

    configuration_ = ChannelPool.WithInstance(channel => new Results.ResultsClient(channel).GetServiceConfiguration(new Empty())
                                                                                           .DataChunkMaxSize);
  }

  private ILoggerFactory LoggerFactory { get; }

  /// <summary>
  ///   Set or Get TaskOptions with inside MaxDuration, Priority, AppName, VersionName and AppNamespace
  /// </summary>
  public TaskOptions TaskOptions { get; }

  /// <summary>
  ///   Get SessionId object stored during the call of SubmitTask, SubmitSubTask,
  ///   SubmitSubTaskWithDependencies or WaitForCompletion, WaitForSubTaskCompletion or GetResults
  /// </summary>
  public Session SessionId { get; }

  /// <summary>
  ///   The channel pool to use for creating clients
  /// </summary>
  public ObjectPool<GrpcChannel> ChannelPool { get; }

  /// <summary>
  ///   The logger to call the generate log in Seq
  /// </summary>

  protected ILogger<T> Logger { get; }

  private async ValueTask<Session> CreateSessionAsync(IEnumerable<string> partitionIds,
                                                      CancellationToken   cancellationToken = default)
  {
    using var _ = Logger.LogFunction();
    Logger.LogDebug("Creating Session... ");
    var createSessionReply = await ChannelPool.WithInstanceAsync(async channel => await new Sessions.SessionsClient(channel).CreateSessionAsync(new CreateSessionRequest
                                                                                                                                                {
                                                                                                                                                  DefaultTaskOption =
                                                                                                                                                    TaskOptions,
                                                                                                                                                  PartitionIds =
                                                                                                                                                  {
                                                                                                                                                    partitionIds,
                                                                                                                                                  },
                                                                                                                                                },
                                                                                                                                                cancellationToken:
                                                                                                                                                cancellationToken)
                                                                                                                            .ConfigureAwait(false),
                                                                 cancellationToken)
                                              .ConfigureAwait(false);
    Logger.LogDebug("Session Created {SessionId}",
                    SessionId);
    return new Session
           {
             Id = createSessionReply.SessionId,
           };
  }


  /// <summary>
  ///   Returns the status of the task
  /// </summary>
  /// <param name="taskId">The taskId of the task</param>
  /// <param name="cancellationToken"></param>
  /// <returns></returns>
  [PublicAPI]
  public async ValueTask<TaskStatus> GetTaskStatusAsync(string            taskId,
                                                        CancellationToken cancellationToken = default)
  {
    var status = await GetTaskStatuesAsync(new[]
                                           {
                                             taskId,
                                           },
                                           cancellationToken)
                       .SingleAsync(cancellationToken)
                       .ConfigureAwait(false);
    return status.Item2;
  }

  /// <summary>
  ///   Returns the status of the task
  /// </summary>
  /// <param name="taskId">The taskId of the task</param>
  /// <returns></returns>
  [PublicAPI]
  public TaskStatus GetTaskStatus(string taskId)
    => GetTaskStatusAsync(taskId)
      .WaitSync();


  /// <summary>
  ///   Returns the list status of the tasks
  /// </summary>
  /// <param name="taskIds">The list of taskIds</param>
  /// <param name="cancellationToken"></param>
  /// <returns></returns>
  [PublicAPI]
  public IAsyncEnumerable<Tuple<string, TaskStatus>> GetTaskStatuesAsync(CancellationToken cancellationToken = default,
                                                                         params string[]   taskIds)
    => GetTaskStatuesAsync(taskIds,
                           cancellationToken);

  /// <summary>
  ///   Returns the list status of the tasks
  /// </summary>
  /// <param name="taskIds">The list of taskIds</param>
  /// <param name="cancellationToken"></param>
  /// <returns></returns>
  private async IAsyncEnumerable<Tuple<string, TaskStatus>> GetTaskStatuesAsync(string[]                                   taskIds,
                                                                                [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    await using var channel = await ChannelPool.GetAsync(cancellationToken)
                                               .ConfigureAwait(false);
    var tasksClient = new Tasks.TasksClient(channel);
    var tasks = tasksClient.ListTasksAsync(new Filters
                                           {
                                             Or =
                                             {
                                               taskIds.Select(TasksClientExt.TaskIdFilter),
                                             },
                                           },
                                           new ListTasksRequest.Types.Sort
                                           {
                                             Direction = SortDirection.Asc,
                                             Field = new TaskField
                                                     {
                                                       TaskSummaryField = new TaskSummaryField
                                                                          {
                                                                            Field = TaskSummaryEnumField.TaskId,
                                                                          },
                                                     },
                                           },
                                           cancellationToken: cancellationToken);
    await using var taskIterator = tasks.GetAsyncEnumerator(cancellationToken);
    while (true)
    {
      try
      {
        if (!await taskIterator.MoveNextAsync()
                               .ConfigureAwait(false))
        {
          break;
        }
      }
      catch (Exception e)
      {
        channel.RecordException(e);
        throw;
      }

      yield return new Tuple<string, TaskStatus>(taskIterator.Current.Id,
                                                 taskIterator.Current.Status);
    }
  }

  /// <summary>
  ///   Returns the list status of the tasks
  /// </summary>
  /// <param name="taskIds">The list of taskIds</param>
  /// <returns></returns>
  [PublicAPI]
  public IEnumerable<Tuple<string, TaskStatus>> GetTaskStatues(params string[] taskIds)
    => GetTaskStatuesAsync(taskIds)
      .ToEnumerable();

  /// <summary>
  ///   Return the taskOutput when error occurred
  /// </summary>
  /// <param name="taskId"></param>
  /// <param name="cancellationToken"></param>
  /// <returns></returns>

  // TODO: This function should not have Output as a return type because it is a gRPC type
  [PublicAPI]
  public async ValueTask<Output> GetTaskOutputInfoAsync(string            taskId,
                                                        CancellationToken cancellationToken = default)
  {
    var getTaskResponse = await ChannelPool.WithInstanceAsync(async channel => await new Tasks.TasksClient(channel).GetTaskAsync(new GetTaskRequest
                                                                                                                                 {
                                                                                                                                   TaskId = taskId,
                                                                                                                                 })
                                                                                                                   .ConfigureAwait(false),
                                                              cancellationToken)
                                           .ConfigureAwait(false);
    return new Output
           {
             Error = new Output.Types.Error
                     {
                       Details = getTaskResponse.Task.Output.Error,
                     },
           };
  }


  /// <summary>
  ///   Return the taskOutput when error occurred
  /// </summary>
  /// <param name="taskId"></param>
  /// <returns></returns>

  // TODO: This function should not have Output as a return type because it is a gRPC type
  [PublicAPI]
  public Output GetTaskOutputInfo(string taskId)
    => GetTaskOutputInfoAsync(taskId)
      .WaitSync();


  /// <summary>
  ///   The method to submit several tasks with dependencies tasks. This task will wait for
  ///   to start until all dependencies are completed successfully
  /// </summary>
  /// <param name="payloadsWithDependencies">
  ///   A list of Tuple(resultId, payload, parent dependencies) in dependence of those
  ///   created tasks
  /// </param>
  /// <param name="maxRetries">The number of retry before fail to submit task. Default = 5 retries</param>
  /// <param name="taskOptions">TaskOptions overrides if non null override default in Session</param>
  /// <param name="cancellationToken"></param>
  /// <remarks>The result ids must first be created using <see cref="CreateResultsMetadata" /></remarks>
  /// <returns>return a list of taskIds of the created tasks </returns>
  [PublicAPI]
  public IAsyncEnumerable<string> SubmitTasksWithDependenciesAsync(IEnumerable<Tuple<string, byte[], IList<string>>> payloadsWithDependencies,
                                                                   int                                               maxRetries        = 5,
                                                                   TaskOptions?                                      taskOptions       = null,
                                                                   CancellationToken                                 cancellationToken = default)
    => payloadsWithDependencies.ToChunks(chunkSubmitSize_)
                               .ToAsyncEnumerable()
                               .SelectMany(chunk => ChunkSubmitTasksWithDependenciesAsync(chunk.Select(tuple => ((string?)tuple.Item1, tuple.Item2, tuple.Item3,
                                                                                                                 (TaskOptions?)null)),
                                                                                          maxRetries,
                                                                                          taskOptions,
                                                                                          cancellationToken))
                               .Select(task => task.taskId);

  /// <summary>
  ///   The method to submit several tasks with dependencies tasks. This task will wait for
  ///   to start until all dependencies are completed successfully
  /// </summary>
  /// <param name="payloadsWithDependencies">
  ///   A list of Tuple(resultId, payload, parent dependencies) in dependence of those
  ///   created tasks
  /// </param>
  /// <param name="maxRetries">The number of retry before fail to submit task. Default = 5 retries</param>
  /// <param name="taskOptions">TaskOptions overrides if non null override default in Session</param>
  /// <remarks>The result ids must first be created using <see cref="CreateResultsMetadata" /></remarks>
  /// <returns>return a list of taskIds of the created tasks </returns>
  [PublicAPI]
  public IEnumerable<string> SubmitTasksWithDependencies(IEnumerable<Tuple<string, byte[], IList<string>>> payloadsWithDependencies,
                                                         int                                               maxRetries  = 5,
                                                         TaskOptions?                                      taskOptions = null)
    => SubmitTasksWithDependenciesAsync(payloadsWithDependencies,
                                        maxRetries,
                                        taskOptions)
      .ToEnumerable();

  /// <summary>
  ///   The method to submit several tasks with dependencies tasks. This task will wait for
  ///   to start until all dependencies are completed successfully
  /// </summary>
  /// <param name="payloadsWithDependencies">
  ///   A list of Tuple(Payload, parent dependencies) in dependence of those created
  ///   tasks
  /// </param>
  /// <param name="maxRetries">The number of retry before fail to submit task. Default = 5 retries</param>
  /// <param name="taskOptions">
  ///   TaskOptions argument to override default taskOptions in Session.
  ///   If non null it will override the default taskOptions in SessionService for client or given by taskHandler for worker
  /// </param>
  /// <param name="cancellationToken"></param>
  /// <returns>return a list of taskIds of the created tasks </returns>
  [PublicAPI]
  public IAsyncEnumerable<string> SubmitTasksWithDependenciesAsync(IEnumerable<Tuple<byte[], IList<string>>> payloadsWithDependencies,
                                                                   int                                       maxRetries        = 5,
                                                                   TaskOptions?                              taskOptions       = null,
                                                                   CancellationToken                         cancellationToken = default)
    => payloadsWithDependencies.ToChunks(chunkSubmitSize_)
                               .ToAsyncEnumerable()
                               .SelectMany(chunk => ChunkSubmitTasksWithDependenciesAsync(chunk.Select(tuple => ((string?)null, tuple.Item1, tuple.Item2,
                                                                                                                 (TaskOptions?)null)),
                                                                                          maxRetries,
                                                                                          taskOptions,
                                                                                          cancellationToken))
                               .Select(task => task.taskId);

  /// <summary>
  ///   The method to submit several tasks with dependencies tasks. This task will wait for
  ///   to start until all dependencies are completed successfully
  /// </summary>
  /// <param name="payloadsWithDependencies">
  ///   A list of Tuple(Payload, parent dependencies) in dependence of those created
  ///   tasks
  /// </param>
  /// <param name="maxRetries">The number of retry before fail to submit task. Default = 5 retries</param>
  /// <param name="taskOptions">
  ///   TaskOptions argument to override default taskOptions in Session.
  ///   If non null it will override the default taskOptions in SessionService for client or given by taskHandler for worker
  /// </param>
  /// <returns>return a list of taskIds of the created tasks </returns>
  [PublicAPI]
  public IEnumerable<string> SubmitTasksWithDependencies(IEnumerable<Tuple<byte[], IList<string>>> payloadsWithDependencies,
                                                         int                                       maxRetries  = 5,
                                                         TaskOptions?                              taskOptions = null)
    => SubmitTasksWithDependenciesAsync(payloadsWithDependencies,
                                        maxRetries,
                                        taskOptions)
      .ToEnumerable();


  /// <summary>
  ///   The method to submit several tasks with dependencies tasks. This task will wait for
  ///   to start until all dependencies are completed successfully
  /// </summary>
  /// <param name="payloadsWithDependencies">A list of Tuple(resultId, Payload) in dependence of those created tasks</param>
  /// <param name="maxRetries">Set the number of retries Default Value 5</param>
  /// <param name="taskOptions">
  ///   TaskOptions argument to override default taskOptions in Session.
  ///   If non null it will override the default taskOptions in SessionService for client or given by taskHandler for worker
  /// </param>
  /// <param name="cancellationToken"></param>
  /// <returns>return the ids of the created tasks</returns>
  public async IAsyncEnumerable<(string taskId, string resultId)> ChunkSubmitTasksWithDependenciesAsync(
    IEnumerable<(string?, byte[], IList<string>, TaskOptions?)> payloadsWithDependencies,
    int                                                         maxRetries        = 5,
    TaskOptions?                                                taskOptions       = null,
    [EnumeratorCancellation] CancellationToken                  cancellationToken = default)
  {
    using var _ = Logger.LogFunction();

    var taskProperties         = new List<(Either<string, int>, int, bool, IList<string>, TaskOptions?)>();
    var smallPayloadProperties = new List<byte[]>();
    var largePayloadProperties = new List<(byte[], int)>();
    var nbResults              = 0;

    foreach (var (resultId, payload, dependencies, specificTaskOptions) in payloadsWithDependencies)
    {
      Either<string, int> result;
      if (resultId is null)
      {
        result    =  nbResults;
        nbResults += 1;
      }
      else
      {
        result = resultId;
      }

      int  payloadIndex;
      bool isLarge;
      if (payload.Length > configuration_)
      {
        payloadIndex = largePayloadProperties.Count;
        largePayloadProperties.Add((payload, nbResults));
        nbResults += 1;
        isLarge   =  true;
      }
      else
      {
        payloadIndex = smallPayloadProperties.Count;
        smallPayloadProperties.Add(payload);
        isLarge = false;
      }

      taskProperties.Add((result, payloadIndex, isLarge, dependencies, specificTaskOptions));
    }

    var uploadSmallPayloads = smallPayloadProperties.ParallelSelect(new ParallelTaskOptions(properties_.MaxParallelChannels,
                                                                                            cancellationToken),
                                                                    payload => Retry.WhileException(maxRetries,
                                                                                                    2000,
                                                                                                    async _ =>
                                                                                                    {
                                                                                                      await using var channel =
                                                                                                        await ChannelPool.GetAsync(cancellationToken)
                                                                                                                         .ConfigureAwait(false);
                                                                                                      try
                                                                                                      {
                                                                                                        var resultClient = new Results.ResultsClient(channel);
                                                                                                        var response = await resultClient
                                                                                                                             .CreateResultsAsync(new CreateResultsRequest
                                                                                                                                                 {
                                                                                                                                                   SessionId = SessionId
                                                                                                                                                     .Id,
                                                                                                                                                   Results =
                                                                                                                                                   {
                                                                                                                                                     new
                                                                                                                                                     CreateResultsRequest
                                                                                                                                                     .Types.ResultCreate
                                                                                                                                                     {
                                                                                                                                                       Data =
                                                                                                                                                         UnsafeByteOperations
                                                                                                                                                           .UnsafeWrap(payload),
                                                                                                                                                     },
                                                                                                                                                   },
                                                                                                                                                 },
                                                                                                                                                 cancellationToken:
                                                                                                                                                 cancellationToken)
                                                                                                                             .ConfigureAwait(false);

                                                                                                        return response.Results.Single()
                                                                                                                       .ResultId;
                                                                                                      }
                                                                                                      catch (Exception e)
                                                                                                      {
                                                                                                        channel.RecordException(e);
                                                                                                        throw;
                                                                                                      }
                                                                                                    },
                                                                                                    true,
                                                                                                    Logger,
                                                                                                    cancellationToken,
                                                                                                    typeof(IOException),
                                                                                                    typeof(RpcException))
                                                                                    .AsTask())
                                                    .ToListAsync(cancellationToken);

    var createResultMetadata = Retry.WhileException(maxRetries,
                                                    2000,
                                                    async _ =>
                                                    {
                                                      await using var channel = await ChannelPool.GetAsync(cancellationToken)
                                                                                                 .ConfigureAwait(false);
                                                      try
                                                      {
                                                        var resultClient = new Results.ResultsClient(channel);
                                                        var response = await resultClient.CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                                                                                     {
                                                                                                                       SessionId = SessionId.Id,
                                                                                                                       Results =
                                                                                                                       {
                                                                                                                         Enumerable.Range(0,
                                                                                                                                          nbResults)
                                                                                                                                   .Select(_ => new
                                                                                                                                             CreateResultsMetaDataRequest
                                                                                                                                             .Types.ResultCreate()),
                                                                                                                       },
                                                                                                                     },
                                                                                                                     cancellationToken: cancellationToken)
                                                                                         .ConfigureAwait(false);

                                                        return response.Results.Select(result => result.ResultId)
                                                                       .AsIList();
                                                      }
                                                      catch (Exception e)
                                                      {
                                                        channel.RecordException(e);
                                                        throw;
                                                      }
                                                    },
                                                    true,
                                                    Logger,
                                                    cancellationToken,
                                                    typeof(IOException),
                                                    typeof(RpcException))
                                    .AsTask();


    var uploadLargePayloads = largePayloadProperties.ParallelForEach(new ParallelTaskOptions(properties_.MaxParallelChannels,
                                                                                             cancellationToken),
                                                                     async payload =>
                                                                     {
                                                                       var results = await createResultMetadata.ConfigureAwait(false);

                                                                       await Retry.WhileException(maxRetries,
                                                                                                  2000,
                                                                                                  async _ =>
                                                                                                  {
                                                                                                    var resultId = results[payload.Item2];
                                                                                                    await using var channel =
                                                                                                      await ChannelPool.GetAsync(cancellationToken)
                                                                                                                       .ConfigureAwait(false);
                                                                                                    try
                                                                                                    {
                                                                                                      var resultClient = new Results.ResultsClient(channel);

                                                                                                      await resultClient.UploadResultData(SessionId.Id,
                                                                                                                                          resultId,
                                                                                                                                          payload.Item1)
                                                                                                                        .ConfigureAwait(false);
                                                                                                    }
                                                                                                    catch (Exception e)
                                                                                                    {
                                                                                                      channel.RecordException(e);
                                                                                                      throw;
                                                                                                    }
                                                                                                  },
                                                                                                  true,
                                                                                                  Logger,
                                                                                                  cancellationToken,
                                                                                                  typeof(IOException),
                                                                                                  typeof(RpcException))
                                                                                  .ConfigureAwait(false);
                                                                     });

    var results       = await createResultMetadata.ConfigureAwait(false);
    var smallPayloads = await uploadSmallPayloads.ConfigureAwait(false);

    var tasks = taskProperties.Select(tuple =>
                                      {
                                        var (result, payloadIndex, isLarge, dependencies, specificTaskOptions) = tuple;
                                        var resultId = (string?)result ?? results[(int)result]!;

                                        var payloadId = isLarge
                                                          ? results[largePayloadProperties[payloadIndex].Item2]
                                                          : smallPayloads[payloadIndex];

                                        return new SubmitTasksRequest.Types.TaskCreation
                                               {
                                                 PayloadId = payloadId,
                                                 DataDependencies =
                                                 {
                                                   dependencies,
                                                 },
                                                 ExpectedOutputKeys =
                                                 {
                                                   resultId,
                                                 },
                                                 TaskOptions = specificTaskOptions,
                                               };
                                      })
                              .AsIList();

    var taskSubmit = tasks.ToChunks(100)
                          .ParallelSelect(new ParallelTaskOptions(1,
                                                                  cancellationToken),
                                          async taskChunk =>
                                          {
                                            var response = await Retry.WhileException(maxRetries,
                                                                                      2000,
                                                                                      async _ =>
                                                                                      {
                                                                                        await using var channel = await ChannelPool.GetAsync(cancellationToken)
                                                                                                                                   .ConfigureAwait(false);
                                                                                        try
                                                                                        {
                                                                                          var taskClient = new Tasks.TasksClient(channel);

                                                                                          return await taskClient.SubmitTasksAsync(new SubmitTasksRequest
                                                                                                                                   {
                                                                                                                                     TaskOptions = taskOptions,
                                                                                                                                     SessionId   = SessionId.Id,
                                                                                                                                     TaskCreations =
                                                                                                                                     {
                                                                                                                                       taskChunk,
                                                                                                                                     },
                                                                                                                                   },
                                                                                                                                   cancellationToken: cancellationToken)
                                                                                                                 .ConfigureAwait(false);
                                                                                        }
                                                                                        catch (Exception e)
                                                                                        {
                                                                                          channel.RecordException(e);
                                                                                          throw;
                                                                                        }
                                                                                      },
                                                                                      true,
                                                                                      Logger,
                                                                                      cancellationToken,
                                                                                      typeof(IOException),
                                                                                      typeof(RpcException))
                                                                      .ConfigureAwait(false);

                                            return response.TaskInfos.Select(task => (task.TaskId, task.ExpectedOutputIds.Single()));
                                          });

    await foreach (var taskChunk in taskSubmit.ConfigureAwait(false))
    {
      foreach (var task in taskChunk)
      {
        yield return task;
      }
    }

    await uploadLargePayloads.ConfigureAwait(false);
  }

  /// <summary>
  ///   User method to wait for only the parent task from the client
  /// </summary>
  /// <param name="taskId">
  ///   The task taskId of the task to wait for
  /// </param>
  /// <param name="maxRetries">Max number of retries for the underlying calls</param>
  /// <param name="delayMs">Delay between retries</param>
  /// <param name="cancellationToken"></param>
  [PublicAPI]
  public async ValueTask WaitForTaskCompletionAsync(string            taskId,
                                                    int               maxRetries        = 5,
                                                    int               delayMs           = 20000,
                                                    CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(taskId);

    await WaitForTasksCompletionAsync(new[]
                                      {
                                        taskId,
                                      },
                                      maxRetries,
                                      delayMs,
                                      cancellationToken)
      .ConfigureAwait(false);
  }

  /// <summary>
  ///   User method to wait for only the parent task from the client
  /// </summary>
  /// <param name="taskId">
  ///   The task taskId of the task to wait for
  /// </param>
  /// <param name="maxRetries">Max number of retries for the underlying calls</param>
  /// <param name="delayMs">Delay between retries</param>
  [PublicAPI]
  public void WaitForTaskCompletion(string taskId,
                                    int    maxRetries = 5,
                                    int    delayMs    = 20000)
    => WaitForTaskCompletionAsync(taskId,
                                  maxRetries,
                                  delayMs)
      .WaitSync();

  /// <summary>
  ///   User method to wait for only the parent task from the client
  /// </summary>
  /// <param name="taskIds">
  ///   List of taskIds
  /// </param>
  /// <param name="maxRetries">Max number of retries</param>
  /// <param name="delayMs"></param>
  /// <param name="cancellationToken"></param>
  [PublicAPI]
  public async ValueTask WaitForTasksCompletionAsync(IEnumerable<string> taskIds,
                                                     int                 maxRetries        = 5,
                                                     int                 delayMs           = 20000,
                                                     CancellationToken   cancellationToken = default)
  {
    using var _ = Logger.LogFunction();

    var filter = new TaskFilter
                 {
                   Task = new TaskFilter.Types.IdsRequest
                          {
                            Ids =
                            {
                              taskIds,
                            },
                          },
                 };

    await Retry.WhileException(maxRetries,
                               delayMs,
                               async retry =>
                               {
                                 await using var channel = await ChannelPool.GetAsync(cancellationToken)
                                                                            .ConfigureAwait(false);
                                 try
                                 {
                                   var submitterService = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(channel);

                                   if (retry > 1)
                                   {
                                     Logger.LogWarning("Try {try} for {funcName}",
                                                       retry,
                                                       nameof(submitterService.WaitForCompletion));
                                   }

                                   var __ = await submitterService.WaitForCompletionAsync(new WaitRequest
                                                                                          {
                                                                                            Filter                      = filter,
                                                                                            StopOnFirstTaskCancellation = true,
                                                                                            StopOnFirstTaskError        = true,
                                                                                          },
                                                                                          cancellationToken: cancellationToken)
                                                                  .ConfigureAwait(false);
                                 }
                                 catch (Exception e)
                                 {
                                   channel.RecordException(e);
                                   throw;
                                 }
                               },
                               true,
                               Logger,
                               typeof(IOException),
                               typeof(RpcException))
               .ConfigureAwait(false);
  }

  /// <summary>
  ///   User method to wait for only the parent task from the client
  /// </summary>
  /// <param name="taskIds">
  ///   List of taskIds
  /// </param>
  /// <param name="maxRetries">Max number of retries</param>
  /// <param name="delayMs"></param>
  [PublicAPI]
  public void WaitForTasksCompletion(IEnumerable<string> taskIds,
                                     int                 maxRetries = 5,
                                     int                 delayMs    = 20000)
    => WaitForTasksCompletionAsync(taskIds,
                                   maxRetries,
                                   delayMs)
      .WaitSync();

  /// <summary>
  ///   Get the result status of a list of results
  /// </summary>
  /// <param name="taskIds">Collection of task ids from which to retrieve results</param>
  /// <param name="cancellationToken"></param>
  /// <returns>A ResultCollection sorted by Status Completed, Result in Error or missing</returns>
  [PublicAPI]
  public async ValueTask<ResultStatusCollection> GetResultStatusAsync(IEnumerable<string> taskIds,
                                                                      CancellationToken   cancellationToken = default)
  {
    var taskList = taskIds.ToList();
    var mapTaskResults = await GetResultIdsAsync(taskList,
                                                 cancellationToken)
                           .ConfigureAwait(false);

    var result2TaskDic = mapTaskResults.ToDictionary(result => result.ResultIds.Single(),
                                                     result => result.TaskId);

    var missingTasks = taskList.Count > mapTaskResults.Count
                         ? taskList.Except(result2TaskDic.Values)
                                   .Select(tid => new ResultStatusData(string.Empty,
                                                                       tid,
                                                                       ResultStatus.Notfound))
                         : Array.Empty<ResultStatusData>();

    var idStatus = await Retry.WhileException(5,
                                              2000,
                                              async retry =>
                                              {
                                                Logger.LogDebug("Try {try} for {funcName}",
                                                                retry,
                                                                nameof(Results.ResultsClient.GetResult));

                                                return await result2TaskDic.Keys.ToChunks(100)
                                                                           .ParallelSelect(new ParallelTaskOptions(properties_.MaxParallelChannels,
                                                                                                                   cancellationToken),
                                                                                           async chunk =>
                                                                                           {
                                                                                             await using var channel = await ChannelPool.GetAsync(cancellationToken)
                                                                                                                                        .ConfigureAwait(false);
                                                                                             try
                                                                                             {
                                                                                               var resultsClient = new Results.ResultsClient(channel);
                                                                                               var filters = chunk.Select(resultId => new FiltersAnd
                                                                                                                                      {
                                                                                                                                        And =
                                                                                                                                        {
                                                                                                                                          new FilterField
                                                                                                                                          {
                                                                                                                                            Field = new ResultField
                                                                                                                                                    {
                                                                                                                                                      ResultRawField =
                                                                                                                                                        new
                                                                                                                                                        ResultRawField
                                                                                                                                                        {
                                                                                                                                                          Field =
                                                                                                                                                            ResultRawEnumField
                                                                                                                                                              .ResultId,
                                                                                                                                                        },
                                                                                                                                                    },
                                                                                                                                            FilterString =
                                                                                                                                              new FilterString
                                                                                                                                              {
                                                                                                                                                Operator =
                                                                                                                                                  FilterStringOperator
                                                                                                                                                    .Equal,
                                                                                                                                                Value = resultId,
                                                                                                                                              },
                                                                                                                                          },
                                                                                                                                        },
                                                                                                                                      });
                                                                                               var res = await resultsClient.ListResultsAsync(new ListResultsRequest
                                                                                                                                              {
                                                                                                                                                Filters =
                                                                                                                                                  new Api.gRPC.V1.Results
                                                                                                                                                  .Filters
                                                                                                                                                  {
                                                                                                                                                    Or =
                                                                                                                                                    {
                                                                                                                                                      filters,
                                                                                                                                                    },
                                                                                                                                                  },
                                                                                                                                                Sort =
                                                                                                                                                  new ListResultsRequest.
                                                                                                                                                  Types.Sort
                                                                                                                                                  {
                                                                                                                                                    Direction =
                                                                                                                                                      SortDirection.Asc,
                                                                                                                                                    Field =
                                                                                                                                                      new ResultField
                                                                                                                                                      {
                                                                                                                                                        ResultRawField =
                                                                                                                                                          new
                                                                                                                                                          ResultRawField
                                                                                                                                                          {
                                                                                                                                                            Field =
                                                                                                                                                              ResultRawEnumField
                                                                                                                                                                .ResultId,
                                                                                                                                                          },
                                                                                                                                                      },
                                                                                                                                                  },
                                                                                                                                                PageSize = 100,
                                                                                                                                              },
                                                                                                                                              cancellationToken:
                                                                                                                                              cancellationToken)
                                                                                                                            .ConfigureAwait(false);
                                                                                               return res;
                                                                                             }
                                                                                             catch (Exception e)
                                                                                             {
                                                                                               channel.RecordException(e);
                                                                                               throw;
                                                                                             }
                                                                                           })
                                                                           .SelectMany(results => results
                                                                                                  .Results.Select(result => (resultId: result.ResultId,
                                                                                                                             status: result.Status))
                                                                                                  .ToAsyncEnumerable())
                                                                           .ToListAsync(cancellationToken)
                                                                           .ConfigureAwait(false);
                                              },
                                              true,
                                              Logger,
                                              typeof(IOException),
                                              typeof(RpcException))
                              .ConfigureAwait(false);

    var idsResultError = new List<ResultStatusData>();
    var idsReady       = new List<ResultStatusData>();
    var idsNotReady    = new List<ResultStatusData>();

    foreach (var idStatusPair in idStatus)
    {
      var resData = new ResultStatusData(idStatusPair.resultId,
                                         result2TaskDic[idStatusPair.resultId],
                                         idStatusPair.status);

      switch (idStatusPair.status)
      {
        case ResultStatus.Notfound:
          continue;
        case ResultStatus.Completed:
          idsReady.Add(resData);
          break;
        case ResultStatus.Created:
          idsNotReady.Add(resData);
          break;
        case ResultStatus.Unspecified:
        case ResultStatus.Aborted:
        default:
          idsResultError.Add(resData);
          break;
      }

      result2TaskDic.Remove(idStatusPair.resultId);
    }

    var resultStatusList = new ResultStatusCollection(idsReady,
                                                      idsResultError,
                                                      result2TaskDic.Values.ToList(),
                                                      idsNotReady,
                                                      missingTasks.ToList());

    return resultStatusList;
  }


  /// <summary>
  ///   Get the result status of a list of results
  /// </summary>
  /// <param name="taskIds">Collection of task ids from which to retrieve results</param>
  /// <param name="cancellationToken"></param>
  /// <returns>A ResultCollection sorted by Status Completed, Result in Error or missing</returns>
  [PublicAPI]
  public ResultStatusCollection GetResultStatus(IEnumerable<string> taskIds,
                                                CancellationToken   cancellationToken = default)
    => GetResultStatusAsync(taskIds,
                            cancellationToken)
      .WaitSync();

  /// <summary>
  ///   Gets the result ids for a given list of task ids.
  /// </summary>
  /// <param name="taskIds">The list of task ids.</param>
  /// <param name="cancellationToken"></param>
  /// <returns>A collection of map task results.</returns>
  [PublicAPI]
  public ValueTask<ICollection<GetResultIdsResponse.Types.MapTaskResult>> GetResultIdsAsync(IEnumerable<string> taskIds,
                                                                                            CancellationToken   cancellationToken = default)
    => Retry.WhileException(5,
                            2000,
                            async retry =>
                            {
                              if (retry > 1)
                              {
                                Logger.LogWarning("Try {try} for {funcName}",
                                                  retry,
                                                  nameof(GetResultIds));
                              }

                              await using var channel = await ChannelPool.GetAsync(cancellationToken)
                                                                         .ConfigureAwait(false);
                              try
                              {
                                var taskClient = new Tasks.TasksClient(channel);

                                var response = await taskClient.GetResultIdsAsync(new GetResultIdsRequest
                                                                                  {
                                                                                    TaskId =
                                                                                    {
                                                                                      taskIds,
                                                                                    },
                                                                                  },
                                                                                  cancellationToken: cancellationToken)
                                                               .ConfigureAwait(false);

                                return response.TaskResults.AsICollection();
                              }
                              catch (Exception e)
                              {
                                channel.RecordException(e);
                                throw;
                              }
                            },
                            true,
                            Logger,
                            cancellationToken,
                            typeof(IOException),
                            typeof(RpcException));

  /// <summary>
  ///   Gets the result ids for a given list of task ids.
  /// </summary>
  /// <param name="taskIds">The list of task ids.</param>
  /// <returns>A collection of map task results.</returns>
  [PublicAPI]
  public ICollection<GetResultIdsResponse.Types.MapTaskResult> GetResultIds(IEnumerable<string> taskIds)
    => GetResultIdsAsync(taskIds)
      .WaitSync();

  /// <summary>
  ///   Try to find the result of One task. If there no result, the function return byte[0]
  /// </summary>
  /// <param name="taskId">The Id of the task</param>
  /// <param name="cancellationToken">The optional cancellationToken</param>
  /// <returns>Returns the result or byte[0] if there no result</returns>
  [PublicAPI]
  public async ValueTask<byte[]> GetResultAsync(string            taskId,
                                                CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(taskId);

    try
    {
      var results = await GetResultIdsAsync(new[]
                                            {
                                              taskId,
                                            },
                                            cancellationToken)
                      .ConfigureAwait(false);
      var resultId = results.Single()
                            .ResultIds.Single();


      var resultRequest = new ResultRequest
                          {
                            ResultId = resultId,
                            Session  = SessionId.Id,
                          };

      {
        await using var channel = await ChannelPool.GetAsync(cancellationToken)
                                                   .ConfigureAwait(false);
        try
        {
          var eventsClient = new Events.EventsClient(channel);

          await eventsClient.WaitForResultsAsync(SessionId.Id,
                                                 new List<string>
                                                 {
                                                   resultId,
                                                 },
                                                 cancellationToken)
                            .ConfigureAwait(false);
        }
        catch (Exception e)
        {
          channel.RecordException(e);
          throw;
        }
      }

      return await Retry.WhileException(5,
                                        200,
                                        _ => TryGetResultAsync(resultRequest,
                                                               cancellationToken),
                                        true,
                                        cancellationToken,
                                        typeof(IOException),
                                        typeof(RpcException))
                        .ConfigureAwait(false)!;
    }
    catch (Exception ex)
    {
      throw new ClientResultsException($"Cannot retrieve result for task : {taskId}",
                                       ex,
                                       taskId);
    }
  }

  /// <summary>
  ///   Try to find the result of One task. If there no result, the function return byte[0]
  /// </summary>
  /// <param name="taskId">The Id of the task</param>
  /// <param name="cancellationToken">The optional cancellationToken</param>
  /// <returns>Returns the result or byte[0] if there no result</returns>
  [PublicAPI]
  public byte[] GetResult(string            taskId,
                          CancellationToken cancellationToken = default)
    => GetResultAsync(taskId,
                      cancellationToken)
      .WaitSync();

  /// <summary>
  ///   Retrieve results from control plane
  /// </summary>
  /// <param name="taskIds">Collection of task ids</param>
  /// <param name="cancellationToken">The optional cancellationToken</param>
  /// <returns>return a dictionary with key taskId and payload</returns>
  /// <exception cref="ArgumentNullException"></exception>
  /// <exception cref="ArgumentException"></exception>
  [PublicAPI]
  public IAsyncEnumerable<Tuple<string, byte[]>> GetResultsAsync(IEnumerable<string> taskIds,
                                                                 CancellationToken   cancellationToken = default)
    => taskIds.ParallelSelect(async id =>
                              {
                                var res = await GetResultAsync(id,
                                                               cancellationToken)
                                            .ConfigureAwait(false);

                                return new Tuple<string, byte[]>(id,
                                                                 res);
                              });

  /// <summary>
  ///   Retrieve results from control plane
  /// </summary>
  /// <param name="taskIds">Collection of task ids</param>
  /// <param name="cancellationToken">The optional cancellationToken</param>
  /// <returns>return a dictionary with key taskId and payload</returns>
  /// <exception cref="ArgumentNullException"></exception>
  /// <exception cref="ArgumentException"></exception>
  [PublicAPI]
  public IEnumerable<Tuple<string, byte[]>> GetResults(IEnumerable<string> taskIds,
                                                       CancellationToken   cancellationToken = default)
    => GetResultsAsync(taskIds,
                       cancellationToken)
      .ToEnumerable();

  /// <summary>
  ///   Try to get the result if it is available
  /// </summary>
  /// <param name="resultRequest">Request specifying the result to fetch</param>
  /// <param name="cancellationToken">The token used to cancel the operation.</param>
  /// <returns>Returns the result if it is ready, null if task is not yet ready</returns>
  /// <exception cref="Exception"></exception>
  /// <exception cref="ArgumentOutOfRangeException"></exception>
  // TODO: return a compound type to avoid having a nullable that holds the information and return an empty array.
  // TODO: This function should not have an argument of type ResultRequest because it is a gRPC type
  [PublicAPI]
  public async ValueTask<byte[]?> TryGetResultAsync(ResultRequest     resultRequest,
                                                    CancellationToken cancellationToken = default)
  {
    await using var channel = await ChannelPool.GetAsync(cancellationToken)
                                               .ConfigureAwait(false);
    try
    {
      var resultsClient = new Results.ResultsClient(channel);
      var getResultResponse = await resultsClient.GetResultAsync(new GetResultRequest
                                                                 {
                                                                   ResultId = resultRequest.ResultId,
                                                                 },
                                                                 null,
                                                                 null,
                                                                 cancellationToken)
                                                 .ConfigureAwait(false);
      var result = getResultResponse.Result;
      switch (result.Status)
      {
        case ResultStatus.Completed:
        {
          return await resultsClient.DownloadResultData(result.SessionId,
                                                        result.ResultId,
                                                        cancellationToken)
                                    .ConfigureAwait(false);
        }
        case ResultStatus.Aborted:
          throw new Exception($"Error while trying to get result {result.ResultId}. Result was aborted");
        case ResultStatus.Notfound:
          throw new Exception($"Error while trying to get result {result.ResultId}. Result was not found");
        case ResultStatus.Created:
          return null;
        case ResultStatus.Unspecified:
        default:
          throw new ArgumentOutOfRangeException(nameof(result.Status));
      }
    }
    catch (ArgumentOutOfRangeException _)
    {
      throw;
    }
    catch (Exception e)
    {
      channel.RecordException(e);
      throw;
    }
  }

  /// <summary>
  ///   Try to find the result of One task. If there no result, the function return byte[0]
  /// </summary>
  /// <param name="taskId">The Id of the task</param>
  /// <param name="checkOutput"></param>
  /// <param name="cancellationToken">The optional cancellationToken</param>
  /// <returns>Returns the result or byte[0] if there no result or null if task is not yet ready</returns>
  // TODO: return a compound type to avoid having a nullable that holds the information and return an empty array.
  [PublicAPI]
  [Obsolete("Use version without the checkOutput parameter.")]
  public ValueTask<byte[]?> TryGetResultAsync(string            taskId,
                                              bool              checkOutput,
                                              CancellationToken cancellationToken = default)
    => TryGetResultAsync(taskId,
                         cancellationToken);

  /// <summary>
  ///   Try to find the result of One task. If there no result, the function return byte[0]
  /// </summary>
  /// <param name="taskId">The Id of the task</param>
  /// <param name="checkOutput"></param>
  /// <param name="cancellationToken">The optional cancellationToken</param>
  /// <returns>Returns the result or byte[0] if there no result or null if task is not yet ready</returns>
  // TODO: return a compound type to avoid having a nullable that holds the information and return an empty array.
  [PublicAPI]
  [Obsolete("Use version without the checkOutput parameter.")]
  public byte[]? TryGetResult(string            taskId,
                              bool              checkOutput,
                              CancellationToken cancellationToken = default)
    => TryGetResultAsync(taskId,
                         checkOutput,
                         cancellationToken)
      .WaitSync();


  /// <summary>
  ///   Try to find the result of One task. If there no result, the function return byte[0]
  /// </summary>
  /// <param name="taskId">The Id of the task</param>
  /// <param name="cancellationToken">The optional cancellationToken</param>
  /// <returns>Returns the result or byte[0] if there no result or null if task is not yet ready</returns>
  // TODO: return a compound type to avoid having a nullable that holds the information and return an empty array.
  [PublicAPI]
  public async ValueTask<byte[]?> TryGetResultAsync(string            taskId,
                                                    CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(taskId);
    var resultResponse = await GetResultIdsAsync(new[]
                                                 {
                                                   taskId,
                                                 },
                                                 cancellationToken)
                           .ConfigureAwait(false);
    var resultId = resultResponse.Single()
                                 .ResultIds.Single();

    var resultRequest = new ResultRequest
                        {
                          ResultId = resultId,
                          Session  = SessionId.Id,
                        };

    var resultReply = await Retry.WhileException(5,
                                                 2000,
                                                 async retry =>
                                                 {
                                                   if (retry > 1)
                                                   {
                                                     Logger.LogWarning("Try {try} for {funcName}",
                                                                       retry,
                                                                       "SubmitterService.TryGetResultAsync");
                                                   }

                                                   try
                                                   {
                                                     var response = await TryGetResultAsync(resultRequest,
                                                                                            cancellationToken)
                                                                      .ConfigureAwait(false);
                                                     return response;
                                                   }
                                                   catch (AggregateException ex)
                                                   {
                                                     if (ex.InnerException == null)
                                                     {
                                                       throw;
                                                     }

                                                     var rpcException = ex.InnerException;

                                                     switch (rpcException)
                                                     {
                                                       //Not yet available return from the tryGetResult
                                                       case RpcException
                                                            {
                                                              StatusCode: StatusCode.NotFound,
                                                            }:
                                                         return null;

                                                       //We lost the communication rethrow to retry :
                                                       case RpcException
                                                            {
                                                              StatusCode: StatusCode.Unavailable,
                                                            }:
                                                         throw;

                                                       case RpcException
                                                            {
                                                              StatusCode: StatusCode.Aborted or StatusCode.Cancelled,
                                                            }:

                                                         Logger.LogError(rpcException,
                                                                         "Error while trying to get a result: {error}",
                                                                         rpcException.Message);
                                                         return null;
                                                       default:
                                                         throw;
                                                     }
                                                   }
                                                 },
                                                 true,
                                                 Logger,
                                                 cancellationToken,
                                                 typeof(IOException),
                                                 typeof(RpcException));

    return resultReply;
  }

  /// <summary>
  ///   Try to find the result of One task. If there no result, the function return byte[0]
  /// </summary>
  /// <param name="taskId">The Id of the task</param>
  /// <param name="cancellationToken">The optional cancellationToken</param>
  /// <returns>Returns the result or byte[0] if there no result or null if task is not yet ready</returns>
  // TODO: return a compound type to avoid having a nullable that holds the information and return an empty array.
  [PublicAPI]
  public byte[]? TryGetResult(string            taskId,
                              CancellationToken cancellationToken = default)
    => TryGetResultAsync(taskId,
                         cancellationToken)
      .WaitSync();

  /// <summary>
  ///   Try to get result of a list of taskIds
  /// </summary>
  /// <param name="resultIds">A list of result ids</param>
  /// <param name="cancellationToken"></param>
  /// <returns>Returns an Enumerable pair of </returns>
  [PublicAPI]
  public async IAsyncEnumerable<Tuple<string, byte[]>> TryGetResultsAsync(IList<string>                              resultIds,
                                                                          [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var resultStatus = await GetResultStatusAsync(resultIds,
                                                  cancellationToken)
                         .ConfigureAwait(false);

    if (!resultStatus.IdsReady.Any() && !resultStatus.IdsNotReady.Any())
    {
      if (resultStatus.IdsError.Any() || resultStatus.IdsResultError.Any())
      {
        var taskList = string.Join(", ",
                                   resultStatus.IdsResultError.Select(x => x.TaskId));

        if (resultStatus.IdsError.Any())
        {
          if (resultStatus.IdsResultError.Any())
          {
            taskList += ", ";
          }

          taskList += string.Join(", ",
                                  resultStatus.IdsError);
        }

        var taskIdInError = resultStatus.IdsError.Any()
                              ? resultStatus.IdsError[0]
                              : resultStatus.IdsResultError[0].TaskId;

        const string message = "The missing result is in error or canceled. "                                                          +
                               "Please check log for more information on Armonik grid server list of taskIds in Error: [{taskList}]\n" +
                               "1st result id where the task which should create it is in error : {taskIdInError}";

        Logger.LogError(message,
                        taskList,
                        taskIdInError);

        throw new
          ClientResultsException($"The missing result is in error or canceled. Please check log for more information on Armonik grid server list of taskIds in Error: [{taskList}]" +
                                 $"1st result id where the task which should create it is in error : {taskIdInError}",
                                 resultStatus.IdsError.ToArray());
      }
    }

    foreach (var resultStatusData in resultStatus.IdsReady)
    {
      var res = await TryGetResultAsync(new ResultRequest
                                        {
                                          ResultId = resultStatusData.ResultId,
                                          Session  = SessionId.Id,
                                        },
                                        cancellationToken)
                  .ConfigureAwait(false);

      if (res is null)
      {
        continue;
      }

      yield return new Tuple<string, byte[]>(resultStatusData.TaskId,
                                             res);
    }
  }

  /// <summary>
  ///   Try to get result of a list of taskIds
  /// </summary>
  /// <param name="resultIds">A list of result ids</param>
  /// <returns>Returns an Enumerable pair of </returns>
  [PublicAPI]
  public IList<Tuple<string, byte[]>> TryGetResults(IList<string> resultIds)
    => TryGetResultsAsync(resultIds)
       .ToListAsync()
       .WaitSync();

  /// <summary>
  ///   Creates the results metadata
  /// </summary>
  /// <param name="resultNames">Results names</param>
  /// <param name="cancellationToken"></param>
  /// <returns>Dictionary where each result name is associated with its result id</returns>
  [PublicAPI]
  public async ValueTask<Dictionary<string, string>> CreateResultsMetadataAsync(IEnumerable<string> resultNames,
                                                                                CancellationToken   cancellationToken = default)
  {
    await using var channel = await ChannelPool.GetAsync(cancellationToken)
                                               .ConfigureAwait(false);
    try
    {
      var client = new Results.ResultsClient(channel);
      var results = await client.CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                            {
                                                              SessionId = SessionId.Id,
                                                              Results =
                                                              {
                                                                resultNames.Select(name => new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                                           {
                                                                                             Name = name,
                                                                                           }),
                                                              },
                                                            },
                                                            cancellationToken: cancellationToken)
                                .ConfigureAwait(false);
      return results.Results.ToDictionary(r => r.Name,
                                          r => r.ResultId);
    }
    catch (Exception e)
    {
      channel.RecordException(e);
      throw;
    }
  }

  /// <summary>
  ///   Creates the results metadata
  /// </summary>
  /// <param name="resultNames">Results names</param>
  /// <returns>Dictionary where each result name is associated with its result id</returns>
  [PublicAPI]
  public Dictionary<string, string> CreateResultsMetadata(IEnumerable<string> resultNames)
    => CreateResultsMetadataAsync(resultNames)
      .WaitSync();
}
