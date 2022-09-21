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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.DevelopmentKit.Client.Common.Status;
using ArmoniK.DevelopmentKit.Common;
using ArmoniK.DevelopmentKit.Common.Exceptions;

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
  private const int BatchSize = 1000;

  /// <summary>
  ///   Base Object for all Client submitter
  /// </summary>
  /// <param name="loggerFactory">the logger factory to pass for root object</param>
  public BaseClientSubmitter(ILoggerFactory loggerFactory)
    => Logger = loggerFactory.CreateLogger<T>();

  /// <summary>
  ///   Set or Get TaskOptions with inside MaxDuration, Priority, AppName, VersionName and AppNamespace
  /// </summary>
  public TaskOptions TaskOptions { get; set; }

  /// <summary>
  ///   Get SessionId object stored during the call of SubmitTask, SubmitSubTask,
  ///   SubmitSubTaskWithDependencies or WaitForCompletion, WaitForSubTaskCompletion or GetResults
  /// </summary>
  public Session SessionId { get; protected set; }

  /// <summary>
  ///   Protects the access to gRPC service.
  /// </summary>
  private readonly SemaphoreSlim mutex_ = new(1);


#pragma warning restore CS1591

  /// <summary>
  ///   The logger to call the generate log in Seq
  /// </summary>

  protected ILogger<T> Logger { get; set; }

  /// <summary>
  ///   The submitter and receiver Service to submit, wait and get the result
  /// </summary>
  protected Api.gRPC.V1.Submitter.Submitter.SubmitterClient ControlPlaneService { get; set; }

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
    => mutex_.LockedExecute(() => ControlPlaneService.GetTaskStatus(new GetTaskStatusRequest
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
    => mutex_.LockedExecute(() => ControlPlaneService.TryGetTaskOutput(new TaskOutputRequest
                                                                       {
                                                                         TaskId  = taskId,
                                                                         Session = SessionId.Id,
                                                                       }));


  /// <summary>
  ///   The method to submit several tasks with dependencies tasks. This task will wait for
  ///   to start until all dependencies are completed successfully
  /// </summary>
  /// <param name="payloadsWithDependencies">A list of Tuple(taskId, Payload) in dependence of those created tasks</param>
  /// <returns>return a list of taskIds of the created tasks </returns>
  [UsedImplicitly]
  public IEnumerable<string> SubmitTasksWithDependencies(IEnumerable<Tuple<byte[], IList<string>>> payloadsWithDependencies)
    => SubmitTasksWithDependencies(payloadsWithDependencies.Select(payload => Tuple.Create(Guid.NewGuid()
                                                                                               .ToString(),
                                                                                           payload.Item1,
                                                                                           payload.Item2)));

  /// <summary>
  ///   The method to submit several tasks with dependencies tasks. This task will wait for
  ///   to start until all dependencies are completed successfully
  /// </summary>
  /// <param name="payloadsWithDependencies">A list of Tuple(taskId, Payload) in dependence of those created tasks</param>
  /// <param name="maxRetries">Set the number of retries Default Value 5</param>
  /// <returns>return the list of result id that are the output of the tasks </returns>
  [PublicAPI]
  public IEnumerable<string> SubmitTasksWithDependencies(IEnumerable<Tuple<string, byte[], IList<string>>> payloadsWithDependencies,
                                                         int                                               maxRetries = 5)
  {
    using var _                = Logger.LogFunction();
    var       resultIdsCreated = new List<string>();

    using var lockGuard = mutex_.LockGuard();

    var serviceConfiguration = ControlPlaneService.GetServiceConfigurationAsync(new Empty())
                                                  .ResponseAsync.Result;

    for (var nbRetry = 0; nbRetry < maxRetries; nbRetry++)
    {
      resultIdsCreated.Clear();
      try
      {
        using var asyncClientStreamingCall = ControlPlaneService.CreateLargeTasks();

        asyncClientStreamingCall.RequestStream.WriteAsync(new CreateLargeTaskRequest
                                                          {
                                                            InitRequest = new CreateLargeTaskRequest.Types.InitRequest
                                                                          {
                                                                            SessionId   = SessionId.Id,
                                                                            TaskOptions = TaskOptions,
                                                                          },
                                                          })
                                .Wait();


        foreach (var (resultId, payload, dependencies) in payloadsWithDependencies)
        {
          resultIdsCreated.Add(resultId);
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
            Logger.LogDebug("Tasks created : {ids}",
                            string.Join(",",
                                        createTaskReply.CreationStatusList.CreationStatuses));
            break;
          case CreateTaskReply.ResponseOneofCase.Error:
            throw new Exception("Error while creating tasks !");
          default:
            throw new ArgumentOutOfRangeException();
        }

        break;
      }
      catch (Exception e)
      {
        if (nbRetry >= maxRetries)
        {
          throw;
        }

        switch (e)
        {
          case AggregateException
               {
                 InnerException: RpcException,
               } ex:
            Logger.LogWarning(ex.InnerException,
                              "Failure to submit");
            break;
          case AggregateException
               {
                 InnerException: IOException,
               } ex:
            Logger.LogWarning(ex.InnerException,
                              "IOException : Failure to submit, Retrying");
            break;
          case IOException ex:
            Logger.LogWarning(ex,
                              "IOException Failure to submit");
            break;
          default:
            throw;
        }
      }
    }


    Logger.LogDebug("Results created : {ids}",
                    resultIdsCreated);

    return resultIdsCreated;
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
    using var _ = Logger.LogFunction(taskId);

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
  [UsedImplicitly]
  public void WaitForTasksCompletion(IEnumerable<string> taskIds)
  {
    using var _ = Logger.LogFunction();
    Retry.WhileException(5,
                         200,
                         retry =>
                         {
                           Logger.LogDebug("Try {try} for {funcName}",
                                           retry,
                                           nameof(ControlPlaneService.WaitForCompletion));

                           var __ = mutex_.LockedExecute(() => ControlPlaneService.WaitForCompletion(new WaitRequest
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
                                                                                                     }));
                         },
                         true,
                         typeof(IOException),
                         typeof(RpcException));
  }

  /// <summary>
  ///   Get the result status of a list of results
  /// </summary>
  /// <param name="resultIds">Collection of result ids</param>
  /// <param name="cancellationToken"></param>
  /// <returns>A ResultCollection sorted by Status Completed, Result in Error or missing</returns>
  public ResultStatusCollection GetResultStatus(IEnumerable<string> resultIds,
                                                CancellationToken   cancellationToken = default)
  {
    var remainingIds = resultIds.ToHashSet();
    var idStatus = Retry.WhileException(5,
                                        200,
                                        retry =>
                                        {
                                          Logger.LogDebug("Try {try} for {funcName}",
                                                          retry,
                                                          nameof(ControlPlaneService.GetResultStatus));
                                          var resultStatusReply = mutex_.LockedExecute(() => ControlPlaneService.GetResultStatus(new GetResultStatusRequest
                                                                                                                                 {
                                                                                                                                   ResultIds =
                                                                                                                                   {
                                                                                                                                     remainingIds,
                                                                                                                                   },
                                                                                                                                   SessionId = SessionId.Id,
                                                                                                                                 }));
                                          return resultStatusReply.IdStatuses;
                                        },
                                        true,
                                        typeof(IOException),
                                        typeof(RpcException));

    var idsResultError = new List<Tuple<string, ResultStatus>>();
    var idsReady       = new List<Tuple<string, ResultStatus>>();
    var idsNotReady    = new List<Tuple<string, ResultStatus>>();

    foreach (var idStatusPair in idStatus)
    {
      var tuple = Tuple.Create(idStatusPair.ResultId,
                               idStatusPair.Status);
      switch (idStatusPair.Status)
      {
        case ResultStatus.Notfound:
          continue;
        case ResultStatus.Completed:
          idsReady.Add(tuple);
          break;
        case ResultStatus.Created:
          idsNotReady.Add(tuple);
          break;
        case ResultStatus.Unspecified:
        case ResultStatus.Aborted:
        default:
          idsResultError.Add(tuple);
          break;
      }

      remainingIds.Remove(idStatusPair.ResultId);
    }

    var resultStatusList = new ResultStatusCollection
                           {
                             IdsResultError = idsResultError,
                             IdsError       = remainingIds,
                             IdsReady       = idsReady,
                             IdsNotReady    = idsNotReady,
                           };

    return resultStatusList;
  }

  /// <summary>
  ///   Try to find the result of One task. If there no result, the function return byte[0]
  /// </summary>
  /// <param name="resultId">The Id of the result</param>
  /// <param name="cancellationToken">The optional cancellationToken</param>
  /// <returns>Returns the result or byte[0] if there no result</returns>
  public byte[] GetResult(string            resultId,
                          CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(resultId);
    var resultRequest = new ResultRequest
                        {
                          ResultId = resultId,
                          Session  = SessionId.Id,
                        };

    Retry.WhileException(5,
                         200,
                         retry =>
                         {
                           Logger.LogDebug("Try {try} for {funcName}",
                                           retry,
                                           nameof(ControlPlaneService.WaitForAvailability));
                           var availabilityReply = mutex_.LockedExecute(() => ControlPlaneService.WaitForAvailability(resultRequest,
                                                                                                                      cancellationToken: cancellationToken));

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

    var res = TryGetResult(resultId,
                           cancellationToken: cancellationToken);

    if (res != null)
    {
      return res;
    }

    throw new ClientResultsException($"Cannot retrieve result {resultId}",
                                     resultId);
  }


  /// <summary>
  ///   Retrieve results from control plane
  /// </summary>
  /// <param name="resultIds">Collection of result ids</param>
  /// <param name="cancellationToken">The optional cancellationToken</param>
  /// <returns>return a dictionary with key taskId and payload</returns>
  /// <exception cref="ArgumentNullException"></exception>
  /// <exception cref="ArgumentException"></exception>
  public IEnumerable<Tuple<string, byte[]>> GetResults(IEnumerable<string> resultIds,
                                                       CancellationToken   cancellationToken = default)
    => resultIds.AsParallel()
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

    using (await mutex_.LockGuardAsync()
                       .ConfigureAwait(false))
    {
      var streamingCall = ControlPlaneService.TryGetResultStream(resultRequest,
                                                                 cancellationToken: cancellationToken);
      chunks = new List<ReadOnlyMemory<byte>>();
      len    = 0;

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
  /// <param name="resultId">The Id of the result</param>
  /// <param name="checkOutput"></param>
  /// <param name="cancellationToken">The optional cancellationToken</param>
  /// <returns>Returns the result or byte[0] if there no result or null if task is not yet ready</returns>
  [UsedImplicitly]
  public byte[] TryGetResult(string            resultId,
                             bool              checkOutput       = true,
                             CancellationToken cancellationToken = default)
  {
    using var _ = Logger.LogFunction(resultId);
    var resultRequest = new ResultRequest
                        {
                          ResultId = resultId,
                          Session  = SessionId.Id,
                        };

    var resultReply = Retry.WhileException(5,
                                           200,
                                           retry =>
                                           {
                                             Logger.LogDebug("Try {try} for {funcName}",
                                                             retry,
                                                             "ControlPlaneService.TryGetResultAsync");
                                             try
                                             {
                                               var response = TryGetResultAsync(resultRequest,
                                                                                cancellationToken);
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

    return resultReply.Result;
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
          $"The missing result is in error or canceled. Please check log for more information on Armonik grid server list of taskIds in Error : [ {string.Join(", ", resultStatus.IdsResultError.Select(x => x.Item1))}";

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
                                            .Item1;

        msg += $"1st result id where the task which should create it is in error : {taskIdInError}";

        Logger.LogError(msg);

        throw new ClientResultsException(msg,
                                         (resultStatus.IdsError ?? Enumerable.Empty<string>()).Concat(resultStatus.IdsResultError.Select(x => x.Item1)));
      }
    }


    return resultStatus.IdsReady.Select(pair =>
                                        {
                                          var (id, _) = pair;

                                          var res = TryGetResult(id,
                                                                 false);
                                          return res == null
                                                   ? null
                                                   : new Tuple<string, byte[]>(id,
                                                                               res);
                                        })
                       .Where(el => el != null)
                       .ToList();
  }
}
