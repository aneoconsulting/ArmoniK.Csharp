// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2022.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//
// Licensed under the Apache License, Version 2.0 (the "License");
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
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.DevelopmentKit.Client.Common.Status;
using ArmoniK.DevelopmentKit.Common;
using ArmoniK.DevelopmentKit.Common.Exceptions;
using ArmoniK.DevelopmentKit.Common.Extensions;

using Google.Protobuf;

using Grpc.Core;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.DevelopmentKit.Client.Common.Submitter;

/// <summary>
///   Base Object for all Client submitter
///   Need to pass the child object Class Type
/// </summary>
[PublicAPI]
public class BaseClientSubmitter<T>
{
  /// <summary>
  ///   Base Object for all Client submitter
  /// </summary>
  /// <param name="channelPool">Channel used to create grpc clients</param>
  /// <param name="loggerFactory">the logger factory to pass for root object</param>
  /// <param name="chunkSubmitSize">The size of chunk to split the list of tasks</param>
  public BaseClientSubmitter(ChannelPool                channelPool,
                             [CanBeNull] ILoggerFactory loggerFactory   = null,
                             int                        chunkSubmitSize = 500)
  {
    channelPool_     = channelPool;
    Logger           = loggerFactory?.CreateLogger<T>();
    chunkSubmitSize_ = chunkSubmitSize;
  }

  /// <summary>
  ///   Set or Get TaskOptions with inside MaxDuration, Priority, AppName, VersionName and AppNamespace
  /// </summary>
  public TaskOptions TaskOptions { get; set; }

  /// <summary>
  ///   Get SessionId object stored during the call of SubmitTask, SubmitSubTask,
  ///   SubmitSubTaskWithDependencies or WaitForCompletion, WaitForSubTaskCompletion or GetResults
  /// </summary>
  public Session SessionId { get; protected set; }


#pragma warning restore CS1591

  /// <summary>
  ///   The channel pool to use for creating clients
  /// </summary>
  protected ChannelPool channelPool_;


  /// <summary>
  ///   The number of chunk to split the payloadsWithDependencies
  /// </summary>
  private int chunkSubmitSize_;

  /// <summary>
  ///   The logger to call the generate log in Seq
  /// </summary>

  [CanBeNull]
  protected ILogger<T> Logger { get; set; }

  /// <summary>
  ///   Service for interacting with results
  /// </summary>
  protected Results.ResultsClient ResultService { get; set; }

  /// <summary>
  ///   Service for interacting with results
  /// </summary>
  protected Tasks.TasksClient TaskService { get; set; }

  /// <summary>
  ///   Returns the status of the task
  /// </summary>
  /// <param name="taskId">The taskId of the task</param>
  /// <returns></returns>
  public TaskStatus GetTaskStatus(string taskId)
  {
    var status = GetTaskStatues(taskId)
      .Single();
    return status.Item2;
  }

  /// <summary>
  ///   Returns the list status of the tasks
  /// </summary>
  /// <param name="taskIds">The list of taskIds</param>
  /// <returns></returns>
  public IEnumerable<Tuple<string, TaskStatus>> GetTaskStatues(params string[] taskIds)
    => channelPool_.WithChannel(channel => new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(channel).GetTaskStatus(new GetTaskStatusRequest
                                                                                                                      {
                                                                                                                        TaskIds =
                                                                                                                        {
                                                                                                                          taskIds,
                                                                                                                        },
                                                                                                                      })
                                                                                                       .IdStatuses.Select(x => Tuple.Create(x.TaskId,
                                                                                                                                            x.Status)));

  /// <summary>
  ///   Return the taskOutput when error occurred
  /// </summary>
  /// <param name="taskId"></param>
  /// <returns></returns>
  public Output GetTaskOutputInfo(string taskId)
    => channelPool_.WithChannel(channel => new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(channel).TryGetTaskOutput(new TaskOutputRequest
                                                                                                                         {
                                                                                                                           TaskId  = taskId,
                                                                                                                           Session = SessionId.Id,
                                                                                                                         }));

  /// <summary>
  ///   The method to submit several tasks with dependencies tasks. This task will wait for
  ///   to start until all dependencies are completed successfully
  /// </summary>
  /// <param name="payloadsWithDependencies">
  ///   A list of Tuple(resultId, payload, parent dependencies) in dependence of those
  ///   created tasks
  /// </param>
  /// <param name="maxRetries">The number of retry before fail to submit task</param>
  /// <returns>return a list of taskIds of the created tasks </returns>
  [PublicAPI]
  public IEnumerable<string> SubmitTasksWithDependencies(IEnumerable<Tuple<string, byte[], IList<string>>> payloadsWithDependencies,
                                                         int                                               maxRetries = 5)
    => payloadsWithDependencies.ToChunk(chunkSubmitSize_)
                               .SelectMany(chunk => ChunkSubmitTasksWithDependencies(chunk,
                                                                                     maxRetries));

  /// <summary>
  ///   The method to submit several tasks with dependencies tasks. This task will wait for
  ///   to start until all dependencies are completed successfully
  /// </summary>
  /// <param name="payloadsWithDependencies">
  ///   A list of Tuple(Payload, parent dependencies) in dependence of those created
  ///   tasks
  /// </param>
  /// <param name="maxRetries">The number of retry before fail to submit task</param>
  /// <returns>return a list of taskIds of the created tasks </returns>
  [PublicAPI]
  public IEnumerable<string> SubmitTasksWithDependencies(IEnumerable<Tuple<byte[], IList<string>>> payloadsWithDependencies,
                                                         int                                       maxRetries = 5)
    => payloadsWithDependencies.ToChunk(chunkSubmitSize_)
                               .SelectMany(chunk =>
                                           {
                                             return ChunkSubmitTasksWithDependencies(chunk.Select(subPayloadWithDependencies => Tuple.Create(Guid.NewGuid()
                                                                                                                                                 .ToString(),
                                                                                                                                             subPayloadWithDependencies
                                                                                                                                               .Item1,
                                                                                                                                             subPayloadWithDependencies
                                                                                                                                               .Item2)),
                                                                                     maxRetries);
                                           });


  /// <summary>
  ///   The method to submit several tasks with dependencies tasks. This task will wait for
  ///   to start until all dependencies are completed successfully
  /// </summary>
  /// <param name="payloadsWithDependencies">A list of Tuple(resultId, Payload) in dependence of those created tasks</param>
  /// <param name="maxRetries">Set the number of retries Default Value 5</param>
  /// <returns>return the ids of the created tasks</returns>
  [PublicAPI]
  private IEnumerable<string> ChunkSubmitTasksWithDependencies(IEnumerable<Tuple<string, byte[], IList<string>>> payloadsWithDependencies,
                                                               int                                               maxRetries)
  {
    using var _ = Logger?.LogFunction();

    using var channel          = channelPool_.GetChannel();
    var       submitterService = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(channel);

    var serviceConfiguration = submitterService.GetServiceConfigurationAsync(new Empty())
                                               .ResponseAsync.Result;

    for (var nbRetry = 0; nbRetry < maxRetries; nbRetry++)
    {
      try
      {
        using var asyncClientStreamingCall = submitterService.CreateLargeTasks();

        asyncClientStreamingCall.RequestStream.WriteAsync(new CreateLargeTaskRequest
                                                          {
                                                            InitRequest = new CreateLargeTaskRequest.Types.InitRequest
                                                                          {
                                                                            SessionId   = SessionId.Id,
                                                                            TaskOptions = TaskOptions,
                                                                          },
                                                          })
                                .Wait();

        //
        // Here the payloadsWithDependencies can have multiple enumeration
        // It will happen only during a retry to submit tasks
        // Loosing a little bit of perf in case of retry is not a big deal : in the usual case, it should not happen.
        //
        foreach (var (resultId, payload, dependencies) in payloadsWithDependencies)
        {
          asyncClientStreamingCall.RequestStream.WriteAsync(new CreateLargeTaskRequest
                                                            {
                                                              InitTask = new InitTaskRequest
                                                                         {
                                                                           Header = new TaskRequestHeader
                                                                                    {
                                                                                      ExpectedOutputKeys =
                                                                                      {
                                                                                        resultId,
                                                                                      },
                                                                                      DataDependencies =
                                                                                      {
                                                                                        dependencies ?? new List<string>(),
                                                                                      },
                                                                                    },
                                                                         },
                                                            })
                                  .Wait();

          for (var j = 0; j < payload.Length; j += serviceConfiguration.DataChunkMaxSize)
          {
            var chunkSize = Math.Min(serviceConfiguration.DataChunkMaxSize,
                                     payload.Length - j);

            asyncClientStreamingCall.RequestStream.WriteAsync(new CreateLargeTaskRequest
                                                              {
                                                                TaskPayload = new DataChunk
                                                                              {
                                                                                Data = UnsafeByteOperations.UnsafeWrap(payload.AsMemory(j,
                                                                                                                                        chunkSize)),
                                                                              },
                                                              })
                                    .Wait();
          }

          asyncClientStreamingCall.RequestStream.WriteAsync(new CreateLargeTaskRequest
                                                            {
                                                              TaskPayload = new DataChunk
                                                                            {
                                                                              DataComplete = true,
                                                                            },
                                                            })
                                  .Wait();
        }

        asyncClientStreamingCall.RequestStream.WriteAsync(new CreateLargeTaskRequest
                                                          {
                                                            InitTask = new InitTaskRequest
                                                                       {
                                                                         LastTask = true,
                                                                       },
                                                          })
                                .Wait();

        asyncClientStreamingCall.RequestStream.CompleteAsync()
                                .Wait();

        var createTaskReply = asyncClientStreamingCall.ResponseAsync.Result;

        switch (createTaskReply.ResponseCase)
        {
          case CreateTaskReply.ResponseOneofCase.None:
            throw new Exception("Issue with Server !");
          case CreateTaskReply.ResponseOneofCase.CreationStatusList:
            return createTaskReply.CreationStatusList.CreationStatuses.Select(status => status.TaskInfo.TaskId);
          case CreateTaskReply.ResponseOneofCase.Error:
            throw new Exception("Error while creating tasks !");
          default:
            throw new ArgumentOutOfRangeException();
        }
      }
      catch (Exception e)
      {
        if (nbRetry >= maxRetries - 1)
        {
          throw;
        }

        switch (e)
        {
          case AggregateException
               {
                 InnerException: RpcException,
               } ex:
            Logger?.LogWarning(ex.InnerException,
                               "Failure to submit");
            break;
          case AggregateException
               {
                 InnerException: IOException,
               } ex:
            Logger?.LogWarning(ex.InnerException,
                               "IOException : Failure to submit, Retrying");
            break;
          case IOException ex:
            Logger?.LogWarning(ex,
                               "IOException Failure to submit");
            break;
          default:
            Logger.LogError(e,
                            "Unknown failure :");
            throw;
        }
      }

      if (nbRetry > 0)
      {
        Logger?.LogWarning("{retry}/{maxRetries} nbRetry to submit batch of task",
                           nbRetry,
                           maxRetries);
      }
    }

    throw new Exception("Max retry to send has been reached");
  }

  /// <summary>
  ///   User method to wait for only the parent task from the client
  /// </summary>
  /// <param name="taskId">
  ///   The task taskId of the task to wait for
  /// </param>
  /// <param name="retry">Option variable to set the number of retry (Default: 5)</param>
  public void WaitForTaskCompletion(string taskId,
                                    int    retry = 5)
  {
    using var _ = Logger?.LogFunction(taskId);

    WaitForTasksCompletion(new[]
                           {
                             taskId,
                           });
  }

  /// <summary>
  ///   User method to wait for only the parent task from the client
  /// </summary>
  /// <param name="taskIds">
  ///   List of taskIds
  /// </param>
  [PublicAPI]
  public void WaitForTasksCompletion(IEnumerable<string> taskIds)
  {
    using var _ = Logger?.LogFunction();

    using var channel          = channelPool_.GetChannel();
    var       submitterService = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(channel);

    Retry.WhileException(Properties.MaxRetries,
                         Properties.TimeIntervalRetriesInMs,
                         retry =>
                         {
                           if (retry > 1)
                           {
                             Logger?.LogWarning("Try {try} for {funcName}",
                                                retry,
                                                nameof(submitterService.WaitForCompletion));
                           }

                           var __ = submitterService.WaitForCompletion(new WaitRequest
                                                                       {
                                                                         Filter = new TaskFilter
                                                                                  {
                                                                                    Task = new TaskFilter.Types.IdsRequest
                                                                                           {
                                                                                             Ids =
                                                                                             {
                                                                                               taskIds,
                                                                                             },
                                                                                           },
                                                                                  },
                                                                         StopOnFirstTaskCancellation = true,
                                                                         StopOnFirstTaskError        = true,
                                                                       });
                         },
                         true,
                         typeof(IOException),
                         typeof(RpcException));
  }

  /// <summary>
  ///   Get the result status of a list of results
  /// </summary>
  /// <param name="taskIds">Collection of task ids from which to retrieve results</param>
  /// <param name="cancellationToken"></param>
  /// <returns>A ResultCollection sorted by Status Completed, Result in Error or missing</returns>
  public ResultStatusCollection GetResultStatus(IEnumerable<string> taskIds,
                                                CancellationToken   cancellationToken = default)
  {
    var taskList       = taskIds.ToList();
    var mapTaskResults = GetResultIds(taskList);

    var result2TaskDic = mapTaskResults.ToDictionary(result => result.ResultIds.Single(),
                                                     result => result.TaskId);

    var missingTasks = taskList.Count > mapTaskResults.Count
                         ? taskList.Except(result2TaskDic.Values)
                                   .Select(tid => new ResultStatusData(string.Empty,
                                                                       tid,
                                                                       ResultStatus.Notfound))
                         : Array.Empty<ResultStatusData>();

    using var channel          = channelPool_.GetChannel();
    var       submitterService = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(channel);

    var idStatus = Retry.WhileException(Properties.MaxRetries,
                                        Properties.TimeIntervalRetriesInMs,
                                        retry =>
                                        {
                                          Logger?.LogDebug("Try {try} for {funcName}",
                                                           retry,
                                                           nameof(submitterService.GetResultStatus));
                                          var resultStatusReply = submitterService.GetResultStatus(new GetResultStatusRequest
                                                                                                   {
                                                                                                     ResultIds =
                                                                                                     {
                                                                                                       result2TaskDic.Keys,
                                                                                                     },
                                                                                                     SessionId = SessionId.Id,
                                                                                                   });
                                          return resultStatusReply.IdStatuses;
                                        },
                                        true,
                                        typeof(IOException),
                                        typeof(RpcException));

    var idsResultError = new List<ResultStatusData>();
    var idsReady       = new List<ResultStatusData>();
    var idsNotReady    = new List<ResultStatusData>();

    foreach (var idStatusPair in idStatus)
    {
      result2TaskDic.TryGetValue(idStatusPair.ResultId,
                                 out var taskId);

      var resData = new ResultStatusData(idStatusPair.ResultId,
                                         taskId,
                                         idStatusPair.Status);

      switch (idStatusPair.Status)
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

      result2TaskDic.Remove(idStatusPair.ResultId);
    }

    var resultStatusList = new ResultStatusCollection
                           {
                             IdsResultError = idsResultError,
                             IdsError       = result2TaskDic.Values,
                             IdsReady       = idsReady,
                             IdsNotReady    = idsNotReady,
                             Canceled       = missingTasks,
                           };

    return resultStatusList;
  }

  /// <summary>
  ///   Gets the result ids for a given list of task ids.
  /// </summary>
  /// <param name="taskIds">The list of task ids.</param>
  /// <returns>A collection of map task results.</returns>
  public ICollection<GetResultIdsResponse.Types.MapTaskResult> GetResultIds(IEnumerable<string> taskIds)
    => Retry.WhileException(Properties.MaxRetries,
                            Properties.TimeIntervalRetriesInMs,
                            retry =>
                            {
                              if (retry > 1)
                              {
                                Logger?.LogWarning("Try {try} for {funcName}",
                                                   retry,
                                                   nameof(GetResultIds));
                              }

                              return channelPool_.WithChannel(channel => new Tasks.TasksClient(channel).GetResultIds(new GetResultIdsRequest
                                                                                                                     {
                                                                                                                       TaskId =
                                                                                                                       {
                                                                                                                         taskIds,
                                                                                                                       },
                                                                                                                     })
                                                                                                       .TaskResults);
                            },
                            true,
                            typeof(IOException),
                            typeof(RpcException));


  /// <summary>
  ///   Try to find the result of One task. If there no result, the function return byte[0]
  /// </summary>
  /// <param name="taskId">The Id of the task</param>
  /// <param name="cancellationToken">The optional cancellationToken</param>
  /// <returns>Returns the result or byte[0] if there no result</returns>
  public byte[] GetResult(string            taskId,
                          CancellationToken cancellationToken = default)
  {
    using var _ = Logger?.LogFunction(taskId);

    var resultId = GetResultIds(new[]
                                {
                                  taskId,
                                })
                   .Single()
                   .ResultIds.Single();

    var resultRequest = new ResultRequest
                        {
                          ResultId = resultId,
                          Session  = SessionId.Id,
                        };

    using var channel          = channelPool_.GetChannel();
    var       submitterService = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(channel);

    Retry.WhileException(Properties.MaxRetries,
                         Properties.TimeIntervalRetriesInMs,
                         retry =>
                         {
                           Logger?.LogDebug("Try {try} for {funcName}",
                                            retry,
                                            nameof(submitterService.WaitForAvailability));
                           var availabilityReply = submitterService.WaitForAvailability(resultRequest,
                                                                                        cancellationToken: cancellationToken);

                           switch (availabilityReply.TypeCase)
                           {
                             case AvailabilityReply.TypeOneofCase.None:
                               throw new Exception("Issue with Server !");
                             case AvailabilityReply.TypeOneofCase.Ok:
                               break;
                             case AvailabilityReply.TypeOneofCase.Error:
                               throw new
                                 ClientResultsException($"Result in Error - {resultId}\nMessage :\n{string.Join("Inner message:\n", availabilityReply.Error.Errors)}",
                                                        resultId);
                             case AvailabilityReply.TypeOneofCase.NotCompletedTask:
                               throw new DataException($"Result {resultId} was not yet completed");
                             default:
                               throw new ArgumentOutOfRangeException();
                           }
                         },
                         true,
                         typeof(IOException),
                         typeof(RpcException));

    var res = TryGetResultAsync(resultRequest,
                                cancellationToken)
      .Result;

    if (res != null)
    {
      return res;
    }

    throw new ClientResultsException($"Cannot retrieve result for task : {taskId}",
                                     taskId);
  }


  /// <summary>
  ///   Retrieve results from control plane
  /// </summary>
  /// <param name="taskIds">Collection of task ids</param>
  /// <param name="cancellationToken">The optional cancellationToken</param>
  /// <returns>return a dictionary with key taskId and payload</returns>
  /// <exception cref="ArgumentNullException"></exception>
  /// <exception cref="ArgumentException"></exception>
  public IEnumerable<Tuple<string, byte[]>> GetResults(IEnumerable<string> taskIds,
                                                       CancellationToken   cancellationToken = default)
    => taskIds.AsParallel()
              .Select(id =>
                      {
                        var res = GetResult(id,
                                            cancellationToken);

                        return new Tuple<string, byte[]>(id,
                                                         res);
                      });

  /// <summary>
  ///   Try to get the result if it is available
  /// </summary>
  /// <param name="resultRequest">Request specifying the result to fetch</param>
  /// <param name="cancellationToken">The token used to cancel the operation.</param>
  /// <returns></returns>
  /// <exception cref="Exception"></exception>
  /// <exception cref="ArgumentOutOfRangeException"></exception>
  public async Task<byte[]> TryGetResultAsync(ResultRequest     resultRequest,
                                              CancellationToken cancellationToken = default)
  {
    List<ReadOnlyMemory<byte>> chunks;
    int                        len;

    using var channel          = channelPool_.GetChannel();
    var       submitterService = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(channel);

    {
      using var streamingCall = submitterService.TryGetResultStream(resultRequest,
                                                                    cancellationToken: cancellationToken);
      chunks = new List<ReadOnlyMemory<byte>>();
      len    = 0;
      var isPayloadComplete = false;

      while (await streamingCall.ResponseStream.MoveNext(cancellationToken))
      {
        var reply = streamingCall.ResponseStream.Current;

        switch (reply.TypeCase)
        {
          case ResultReply.TypeOneofCase.Result:
            if (!reply.Result.DataComplete)
            {
              chunks.Add(reply.Result.Data.Memory);
              len += reply.Result.Data.Memory.Length;
              // In case we receive a chunk after the data complete message (corrupt stream)
              isPayloadComplete = false;
            }
            else
            {
              isPayloadComplete = true;
            }

            break;
          case ResultReply.TypeOneofCase.None:
            return null;

          case ResultReply.TypeOneofCase.Error:
            throw new Exception($"Error in task {reply.Error.TaskId} {string.Join("Message is : ", reply.Error.Errors.Select(x => x.Detail))}");

          case ResultReply.TypeOneofCase.NotCompletedTask:
            return null;

          default:
            throw new ArgumentOutOfRangeException("Got a reply with an unexpected message type.",
                                                  (Exception)null);
        }
      }

      if (!isPayloadComplete)
      {
        throw new ClientResultsException($"Result data is incomplete for id {resultRequest.ResultId}");
      }
    }

    var res = new byte[len];
    var idx = 0;
    foreach (var rm in chunks)
    {
      rm.CopyTo(res.AsMemory(idx,
                             rm.Length));
      idx += rm.Length;
    }

    return res;
  }


  /// <summary>
  ///   Try to find the result of One task. If there no result, the function return byte[0]
  /// </summary>
  /// <param name="taskId">The Id of the task</param>
  /// <param name="checkOutput"></param>
  /// <param name="cancellationToken">The optional cancellationToken</param>
  /// <returns>Returns the result or byte[0] if there no result or null if task is not yet ready</returns>
  [PublicAPI]
  public byte[] TryGetResult(string            taskId,
                             bool              checkOutput       = true,
                             CancellationToken cancellationToken = default)
  {
    using var _ = Logger?.LogFunction(taskId);
    var resultId = GetResultIds(new[]
                                {
                                  taskId,
                                })
                   .Single()
                   .ResultIds.Single();

    var resultRequest = new ResultRequest
                        {
                          ResultId = resultId,
                          Session  = SessionId.Id,
                        };

    var resultReply = Retry.WhileException(Properties.MaxRetries,
                                           Properties.TimeIntervalRetriesInMs,
                                           retry =>
                                           {
                                             if (retry > 1)
                                             {
                                               Logger?.LogWarning("Try {try} for {funcName}",
                                                                  retry,
                                                                  "SubmitterService.TryGetResultAsync");
                                             }

                                             try
                                             {
                                               var response = TryGetResultAsync(resultRequest,
                                                                                cancellationToken)
                                                 .Result;
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

                                                   Logger?.LogError(rpcException,
                                                                    rpcException.Message);
                                                   return null;
                                                 default:
                                                   throw;
                                               }
                                             }
                                           },
                                           true,
                                           typeof(IOException),
                                           typeof(RpcException));

    return resultReply;
  }

  /// <summary>
  ///   Try to get result of a list of taskIds
  /// </summary>
  /// <param name="resultIds">A list of result ids</param>
  /// <returns>Returns an Enumerable pair of </returns>
  public IList<Tuple<string, byte[]>> TryGetResults(IList<string> resultIds)
  {
    var resultStatus = GetResultStatus(resultIds);

    if (!resultStatus.IdsReady.Any() && !resultStatus.IdsNotReady.Any())
    {
      if (resultStatus.IdsError.Any() || resultStatus.IdsResultError.Any())
      {
        var msg =
          $"The missing result is in error or canceled. Please check log for more information on Armonik grid server list of taskIds in Error : [ {string.Join(", ", resultStatus.IdsResultError.Select(x => x.TaskId))}";

        if (resultStatus.IdsError.Any())
        {
          if (resultStatus.IdsResultError.Any())
          {
            msg += ", ";
          }

          msg += $"{string.Join(", ", resultStatus.IdsError)}";
        }

        msg += " ]\n";

        var taskIdInError = resultStatus.IdsError.Any()
                              ? resultStatus.IdsError.First()
                              : resultStatus.IdsResultError.First()
                                            .TaskId;

        msg += $"1st result id where the task which should create it is in error : {taskIdInError}";

        Logger?.LogError(msg);

        throw new ClientResultsException(msg,
                                         (resultStatus.IdsError ?? Enumerable.Empty<string>()).Concat(resultStatus.IdsResultError.Select(x => x.TaskId)));
      }
    }

    return resultStatus.IdsReady.Select(resultStatusData =>
                                        {
                                          var res = TryGetResultAsync(new ResultRequest
                                                                      {
                                                                        ResultId = resultStatusData.ResultId,
                                                                        Session  = SessionId.Id,
                                                                      })
                                            .Result;
                                          return res == null
                                                   ? null
                                                   : new Tuple<string, byte[]>(resultStatusData.TaskId,
                                                                               res);
                                        })
                       .Where(el => el != null)
                       .ToList();
  }
}
