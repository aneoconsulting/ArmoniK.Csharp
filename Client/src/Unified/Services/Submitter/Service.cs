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
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.DevelopmentKit.Client.Common;
using ArmoniK.DevelopmentKit.Client.Common.Exceptions;
using ArmoniK.DevelopmentKit.Client.Common.Submitter;
using ArmoniK.DevelopmentKit.Client.Unified.Factory;
using ArmoniK.DevelopmentKit.Client.Unified.Services.Common;
using ArmoniK.DevelopmentKit.Common;
using ArmoniK.DevelopmentKit.Common.Exceptions;
using ArmoniK.DevelopmentKit.Common.Extensions;

using Grpc.Core;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.DevelopmentKit.Client.Unified.Services.Submitter;

/// <summary>
///   This class is instantiated by ServiceFactory and allows to execute task on ArmoniK
///   Grid.
/// </summary>
[MarkDownDoc]
public class Service : AbstractClientService, ISubmitterService
{
  private const int MaxRetries = 10;

  // *** you need some mechanism to map types to fields
  private static readonly IDictionary<TaskStatus, ArmonikStatusCode> StatusCodesLookUp = new List<Tuple<TaskStatus, ArmonikStatusCode>>
                                                                                         {
                                                                                           Tuple.Create(TaskStatus.Submitted,
                                                                                                        ArmonikStatusCode.ResultNotReady),
                                                                                           Tuple.Create(TaskStatus.Timeout,
                                                                                                        ArmonikStatusCode.TaskTimeout),
                                                                                           Tuple.Create(TaskStatus.Cancelled,
                                                                                                        ArmonikStatusCode.TaskCancelled),
                                                                                           Tuple.Create(TaskStatus.Cancelling,
                                                                                                        ArmonikStatusCode.TaskCancelled),
                                                                                           Tuple.Create(TaskStatus.Error,
                                                                                                        ArmonikStatusCode.TaskFailed),
                                                                                           Tuple.Create(TaskStatus.Processing,
                                                                                                        ArmonikStatusCode.ResultNotReady),
                                                                                           Tuple.Create(TaskStatus.Dispatched,
                                                                                                        ArmonikStatusCode.ResultNotReady),
                                                                                           Tuple.Create(TaskStatus.Completed,
                                                                                                        ArmonikStatusCode.ResultReady),
                                                                                           Tuple.Create(TaskStatus.Creating,
                                                                                                        ArmonikStatusCode.ResultNotReady),
                                                                                           Tuple.Create(TaskStatus.Unspecified,
                                                                                                        ArmonikStatusCode.TaskFailed),
                                                                                           Tuple.Create(TaskStatus.Processed,
                                                                                                        ArmonikStatusCode.ResultReady),
                                                                                         }.ToDictionary(k => k.Item1,
                                                                                                        v => v.Item2);

  private readonly RequestTaskMap requestTaskMap_ = new();


  private readonly SemaphoreSlim semaphoreSlim_;

  /// <summary>
  ///   The default constructor to open connection with the control plane
  ///   and create the session to ArmoniK
  /// </summary>
  /// <param name="properties">The properties containing TaskOptions and information to communicate with Control plane and </param>
  /// <param name="loggerFactory"></param>
  public Service(Properties                 properties,
                 [CanBeNull] ILoggerFactory loggerFactory = null)
    : base(properties,
           loggerFactory)
  {
    var timeOutSending = properties.TimeTriggerBuffer ?? TimeSpan.FromSeconds(1);

    var maxTasksPerBuffer = properties.MaxTasksPerBuffer;

    semaphoreSlim_ = new SemaphoreSlim(properties.MaxConcurrentBuffers * maxTasksPerBuffer);

    SessionServiceFactory = new SessionServiceFactory(LoggerFactory);

    SessionService = SessionServiceFactory.CreateSession(properties);

    ProtoSerializer = new ProtoSerializer();

    CancellationResultTaskSource = new CancellationTokenSource();
    CancellationQueueTaskSource  = new CancellationTokenSource();

    HandlerResponse = Task.Run(ResultTask,
                               CancellationResultTaskSource.Token);

    Logger = LoggerFactory?.CreateLogger<Service>();
    Logger?.BeginPropertyScope(("SessionId", SessionService.SessionId),
                               ("Class", "Service"));

    BufferSubmit = new BatchUntilInactiveBlock<BlockRequest>(maxTasksPerBuffer,
                                                             timeOutSending,
                                                             new ExecutionDataflowBlockOptions
                                                             {
                                                               BoundedCapacity        = properties.MaxParallelChannels,
                                                               MaxDegreeOfParallelism = properties.MaxParallelChannels,
                                                             });

    BufferSubmit.ExecuteAsync(blockRequests =>
                              {
                                var blockRequestList = blockRequests.ToList();

                                try
                                {
                                  if (blockRequestList.Count == 0)
                                  {
                                    return;
                                  }

                                  Logger?.LogInformation("Submitting buffer of {count} task...",
                                                         blockRequestList.Count);

                                  for (var retry = 0; retry < MaxRetries; retry++)
                                  {
                                    //Generate resultId
                                    foreach (var x in blockRequestList)
                                    {
                                      x.ResultId = Guid.NewGuid();
                                    }

                                    try
                                    {
                                      var taskIds =
                                        SessionService.SubmitTasksWithDependencies(blockRequestList.Select(x => new
                                                                                                             Tuple<string, byte[], IList<string>>(x.ResultId.ToString(),
                                                                                                                                                  x.Payload!.Serialize(),
                                                                                                                                                  null)),
                                                                                   1);


                                      var ids            = taskIds.ToList();
                                      var mapTaskResults = SessionService.GetResultIds(ids);
                                      var taskIdsResultIds = mapTaskResults.ToDictionary(result => result.ResultIds.Single(),
                                                                                         result => result.TaskId);


                                      foreach (var pairTaskIdResultId in taskIdsResultIds)
                                      {
                                        var blockRequest = blockRequestList.FirstOrDefault(x => x.ResultId.ToString() == pairTaskIdResultId.Key);
                                        if (blockRequest == null)
                                        {
                                          throw new InvalidOperationException($"Cannot find BlockRequest with result id {pairTaskIdResultId.Value}");
                                        }

                                        ResultHandlerDictionary[pairTaskIdResultId.Value] = blockRequest.Handler;

                                        requestTaskMap_.PutResponse(blockRequest.SubmitId,
                                                                    pairTaskIdResultId.Value);
                                      }

                                      if (ids.Count() > taskIdsResultIds.Count)
                                      {
                                        Logger?.LogWarning("Fail to submit all tasks at once, retry with missing tasks");

                                        throw new Exception("Fail to submit all tasks at once. Retrying...");
                                      }

                                      break;
                                    }
                                    catch (Exception e)
                                    {
                                      if (retry >= MaxRetries - 1)
                                      {
                                        Logger?.LogError(e,
                                                         "Fail to retry {count} times of submission. Stop trying to submit",
                                                         MaxRetries);
                                        throw;
                                      }

                                      Logger?.LogWarning(e,
                                                         "Fail to submit, {retry}/{maxRetries} retrying",
                                                         retry,
                                                         MaxRetries);

                                      //Delay before submission
                                      Task.Delay(TimeSpan.FromMilliseconds(Properties.TimeIntervalRetriesInMs));
                                    }
                                  }


                                  blockRequestList.ForEach(x =>
                                                           {
                                                             x.Lock?.Release();
                                                           });
                                }
                                catch (Exception e)
                                {
                                  Logger?.LogError(e,
                                                   "Fail to submit buffer with {count} tasks inside",
                                                   blockRequestList?.Count);

                                  requestTaskMap_.BufferFailures(blockRequestList.Select(block => block.SubmitId),
                                                                 e);
                                }
                              });
  }

  private CancellationTokenSource CancellationQueueTaskSource { get; }


  private BatchUntilInactiveBlock<BlockRequest> BufferSubmit { get; }

  /// <summary>
  ///   Property Get the SessionId
  /// </summary>
  [PublicAPI]
  public SessionService SessionService { get; set; }

  [CanBeNull]
  private ILogger Logger { get; }

  private ProtoSerializer ProtoSerializer { get; }

  private SessionServiceFactory SessionServiceFactory { get; set; }

  private CancellationTokenSource CancellationResultTaskSource { get; }

  /// <summary>
  ///   The handler to send the response
  /// </summary>
  public Task HandlerResponse { get; set; }

  /// <summary>
  ///   The sessionId
  /// </summary>
  public string SessionId
    => SessionService?.SessionId.Id;

  /// <summary>
  ///   The method submit will execute task asynchronously on the server and will serialize object[] for Service method
  ///   MethodName(Object[] arguments)
  /// </summary>
  /// <param name="methodName">The name of the method inside the service</param>
  /// <param name="arguments">A list of object that can be passed in parameters of the function</param>
  /// <param name="handler">The handler callBack implemented as IServiceInvocationHandler to get response or result or error</param>
  /// <returns>Return the taskId string</returns>
  public string Submit(string                    methodName,
                       object[]                  arguments,
                       IServiceInvocationHandler handler)
  {
    ArmonikPayload payload = new()
                             {
                               MethodName    = methodName,
                               ClientPayload = ProtoSerializer.SerializeMessageObjectArray(arguments),
                             };
    var taskId = SessionService.SubmitTask(payload.Serialize());
    ResultHandlerDictionary[taskId] = handler;
    return taskId;
  }

  /// <summary>
  ///   The method submit list of task with Enumerable list of arguments that will be serialized to each call of byte[]
  ///   MethodName(byte[] argument)
  /// </summary>
  /// <param name="methodName">The name of the method inside the service</param>
  /// <param name="arguments">A list of parameters that can be passed in parameters of the each call of function</param>
  /// <param name="handler">The handler callBack implemented as IServiceInvocationHandler to get response or result or error</param>
  /// <returns>Return the list of created taskIds</returns>
  public IEnumerable<string> Submit(string                    methodName,
                                    IEnumerable<object[]>     arguments,
                                    IServiceInvocationHandler handler)
  {
    var armonikPayloads = arguments.Select(args => new ArmonikPayload
                                                   {
                                                     MethodName          = methodName,
                                                     ClientPayload       = ProtoSerializer.SerializeMessageObjectArray(args),
                                                     SerializedArguments = false,
                                                   });

    var taskIds   = SessionService.SubmitTasks(armonikPayloads.Select(p => p.Serialize()));
    var submitted = taskIds as string[] ?? taskIds.ToArray();
    foreach (var taskid in submitted)
    {
      ResultHandlerDictionary[taskid] = handler;
    }

    return submitted;
  }

  /// <summary>
  ///   The method submit with One serialized argument that will be already serialized for byte[] MethodName(byte[]
  ///   argument).
  /// </summary>
  /// <param name="methodName">The name of the method inside the service</param>
  /// <param name="argument">One serialized argument that will already serialize for MethodName.</param>
  /// <param name="handler">The handler callBack implemented as IServiceInvocationHandler to get response or result or error</param>
  /// <returns>Returns the taskId string</returns>
  public string Submit(string                    methodName,
                       byte[]                    argument,
                       IServiceInvocationHandler handler)
  {
    ArmonikPayload payload = new()
                             {
                               MethodName          = methodName,
                               ClientPayload       = argument,
                               SerializedArguments = true,
                             };

    var taskId = SessionService.SubmitTask(payload.Serialize());
    ResultHandlerDictionary[taskId] = handler;
    return taskId;
  }

  /// <summary>
  ///   The method submit list of task with Enumerable list of serialized arguments that will be already serialized for
  ///   byte[] MethodName(byte[] argument).
  /// </summary>
  /// <param name="methodName">The name of the method inside the service</param>
  /// <param name="arguments">List of serialized arguments that will already serialize for MethodName.</param>
  /// <param name="handler">The handler callBack implemented as IServiceInvocationHandler to get response or result or error</param>
  /// <returns>Return the taskId string</returns>
  public IEnumerable<string> Submit(string                    methodName,
                                    IEnumerable<byte[]>       arguments,
                                    IServiceInvocationHandler handler)
  {
    var armonikPayloads = arguments.Select(args => new ArmonikPayload
                                                   {
                                                     MethodName          = methodName,
                                                     ClientPayload       = args,
                                                     SerializedArguments = true,
                                                   });

    var taskIds   = SessionService.SubmitTasks(armonikPayloads.Select(p => p.Serialize()));
    var submitted = taskIds as string[] ?? taskIds.ToArray();
    foreach (var taskid in submitted)
    {
      ResultHandlerDictionary[taskid] = handler;
    }

    return submitted;
  }


  /// <summary>
  ///   The method submitAsync will serialize argument in object[] MethodName(object[]  argument).
  /// </summary>
  /// <param name="methodName">The name of the method inside the service</param>
  /// <param name="argument">One serialized argument that will already serialize for MethodName.</param>
  /// <param name="handler">The handler callBack implemented as IServiceInvocationHandler to get response or result or error</param>
  /// <param name="token">The cancellation token</param>
  /// <returns>Returns the taskId string</returns>
  public async Task<string> SubmitAsync(string                    methodName,
                                        object[]                  argument,
                                        IServiceInvocationHandler handler,
                                        CancellationToken         token = default)
  {
    await semaphoreSlim_.WaitAsync(token);

    var blockRequest = new BlockRequest
                       {
                         SubmitId = Guid.NewGuid(),
                         Payload = new ArmonikPayload
                                   {
                                     MethodName          = methodName,
                                     ClientPayload       = ProtoSerializer.SerializeMessageObjectArray(argument),
                                     SerializedArguments = false,
                                   },
                         Handler = handler,
                         Lock    = semaphoreSlim_,
                       };

    return await SubmitAsync(blockRequest,
                             token);
  }

  /// <summary>
  ///   The method submit with one serialized argument that will send as byte[] MethodName(byte[]   argument).
  /// </summary>
  /// <param name="methodName">The name of the method inside the service</param>
  /// <param name="argument">One serialized argument that will already serialize for MethodName.</param>
  /// <param name="handler">The handler callBack implemented as IServiceInvocationHandler to get response or result or error</param>
  /// <param name="token">The cancellation token to set to cancel the async task</param>
  /// <returns>Return the taskId string</returns>
  public async Task<string> SubmitAsync(string                    methodName,
                                        byte[]                    argument,
                                        IServiceInvocationHandler handler,
                                        CancellationToken         token = default)
  {
    await semaphoreSlim_.WaitAsync(token);

    return await SubmitAsync(new BlockRequest
                             {
                               SubmitId = Guid.NewGuid(),
                               Payload = new ArmonikPayload
                                         {
                                           MethodName          = methodName,
                                           ClientPayload       = argument,
                                           SerializedArguments = true,
                                         },
                               Handler = handler,
                               Lock    = semaphoreSlim_,
                             },
                             token)
             .ConfigureAwait(false);
  }

  private async Task<string> SubmitAsync(BlockRequest      blockRequest,
                                         CancellationToken token = default)
  {
    await BufferSubmit.SendAsync(blockRequest,
                                 token)
                      .ConfigureAwait(false);

    return await requestTaskMap_.GetResponseAsync(blockRequest.SubmitId);
  }

  /// <summary>
  ///   This function execute code locally with the same configuration as Armonik Grid execution
  ///   The method needs the Service to execute, the method name to call and arguments of method to pass
  /// </summary>
  /// <param name="service">The instance of object containing the method to call</param>
  /// <param name="methodName">The string name of the method</param>
  /// <param name="arguments">the array of object to pass as arguments for the method</param>
  /// <returns>Returns an object as result of the method call</returns>
  /// <exception cref="WorkerApiException"></exception>
  [CanBeNull]
  public ServiceResult LocalExecute(object   service,
                                    string   methodName,
                                    object[] arguments)
  {
    var methodInfo = service.GetType()
                            .GetMethod(methodName);

    if (methodInfo == null)
    {
      throw new InvalidOperationException($"MethodName [{methodName}] was not found");
    }

    var result = methodInfo.Invoke(service,
                                   arguments);

    return new ServiceResult
           {
             TaskId = Guid.NewGuid()
                          .ToString(),
             Result = result,
           };
  }

  /// <summary>
  ///   This method is used to execute task and waiting after the result.
  ///   the method will return the result of the execution until the grid returns the task result
  /// </summary>
  /// <param name="methodName">The string name of the method</param>
  /// <param name="arguments">the array of object to pass as arguments for the method</param>
  /// <returns>Returns a tuple with the taskId string and an object as result of the method call</returns>
  public ServiceResult Execute(string   methodName,
                               object[] arguments)
  {
    ArmonikPayload unifiedPayload = new()
                                    {
                                      MethodName    = methodName,
                                      ClientPayload = ProtoSerializer.SerializeMessageObjectArray(arguments),
                                    };

    var taskId = SessionService.SubmitTask(unifiedPayload.Serialize());

    var result = ProtoSerializer.DeSerializeMessageObjectArray(SessionService.GetResult(taskId));

    return new ServiceResult
           {
             TaskId = taskId,
             Result = result?[0],
           };
  }

  /// <summary>
  ///   This method is used to execute task and waiting after the result.
  ///   the method will return the result of the execution until the grid returns the task result
  /// </summary>
  /// <param name="methodName">The string name of the method</param>
  /// <param name="dataArg">the array of byte to pass as argument for the methodName(byte[] dataArg)</param>
  /// <returns>Returns a tuple with the taskId string and an object as result of the method call</returns>
  public ServiceResult Execute(string methodName,
                               byte[] dataArg)
  {
    ArmonikPayload unifiedPayload = new()
                                    {
                                      MethodName          = methodName,
                                      ClientPayload       = dataArg,
                                      SerializedArguments = true,
                                    };

    var      taskId = "not-TaskId";
    object[] result;

    try
    {
      taskId = SessionService.SubmitTask(unifiedPayload.Serialize());

      result = ProtoSerializer.DeSerializeMessageObjectArray(SessionService.GetResult(taskId));
    }
    catch (Exception e)
    {
      var status = SessionService.GetTaskStatus(taskId);

      var details = "";

      if (status != TaskStatus.Completed)
      {
        var output = SessionService.GetTaskOutputInfo(taskId);
        details = output.TypeCase == Output.TypeOneofCase.Error
                    ? output.Error.Details
                    : e.Message + e.StackTrace;
      }

      throw new ServiceInvocationException(e is AggregateException
                                             ? e.InnerException
                                             : e,
                                           StatusCodesLookUp.Keys.Contains(status)
                                             ? StatusCodesLookUp[status]
                                             : ArmonikStatusCode.Unknown)
            {
              OutputDetails = details,
            };
    }

    return new ServiceResult
           {
             TaskId = taskId,
             Result = result?[0],
           };
  }

  /// <summary>
  ///   Retrieves the results for the given taskIds.
  /// </summary>
  /// <param name="taskIds">The taskIds to retrieve results for.</param>
  /// <param name="responseHandler">The action to take when a response is received.</param>
  /// <param name="errorHandler">The action to take when an error occurs.</param>
  /// <param name="chunkResultSize">The size of the chunk to retrieve results in.</param>
  private void ProxyTryGetResults(IEnumerable<string>                taskIds,
                                  Action<string, byte[]>             responseHandler,
                                  Action<string, TaskStatus, string> errorHandler,
                                  int                                chunkResultSize = 200)
  {
    var missing  = taskIds.ToHashSet();
    var holdPrev = missing.Count;
    var waitInSeconds = new List<int>
                        {
                          10,
                          1000,
                          5000,
                          10000,
                          20000,
                          30000,
                        };
    var idx = 0;

    while (missing.Count != 0)
    {
      foreach (var bucket in missing.ToList()
                                    .ToChunk(chunkResultSize))
      {
        var resultStatusCollection = SessionService.GetResultStatus(bucket);

        foreach (var resultStatusData in resultStatusCollection.IdsReady)
        {
          try
          {
            Logger?.LogTrace("Response handler for {taskId}",
                             resultStatusData.TaskId);
            responseHandler(resultStatusData.TaskId,
                            Retry.WhileException(Properties.MaxRetries,
                                                 Properties.TimeIntervalRetriesInMs,
                                                 retry =>
                                                 {
                                                   if (retry > 1)
                                                   {
                                                     Logger?.LogWarning("Try {try} for {funcName}",
                                                                        retry,
                                                                        nameof(SessionService.TryGetResultAsync));
                                                   }

                                                   return SessionService.TryGetResultAsync(new ResultRequest
                                                                                           {
                                                                                             ResultId = resultStatusData.ResultId,
                                                                                             Session  = SessionId,
                                                                                           },
                                                                                           CancellationToken.None)
                                                                        .Result;
                                                 },
                                                 true,
                                                 typeof(IOException),
                                                 typeof(RpcException)));
          }
          catch (Exception e)
          {
            Logger?.LogWarning(e,
                               "Response handler for {taskId} threw an error",
                               resultStatusData.TaskId);
            try
            {
              errorHandler(resultStatusData.TaskId,
                           TaskStatus.Error,
                           e.Message + e.StackTrace);
            }
            catch (Exception e2)
            {
              Logger?.LogError(e2,
                               "An error occured while handling another error: {details}",
                               e);
            }
          }
        }

        missing.ExceptWith(resultStatusCollection.IdsReady.Select(x => x.TaskId));

        foreach (var resultStatusData in resultStatusCollection.IdsResultError)
        {
          string details;

          var taskStatus = SessionService.GetTaskStatus(resultStatusData.TaskId);

          switch (taskStatus)
          {
            case TaskStatus.Cancelling:
            case TaskStatus.Cancelled:
              details = $"Task {resultStatusData.TaskId} was canceled";
              break;
            default:
              var outputInfo = SessionService.GetTaskOutputInfo(resultStatusData.TaskId);
              details = outputInfo.TypeCase == Output.TypeOneofCase.Error
                          ? outputInfo.Error.Details
                          : "Result is in status : " + resultStatusData.Status + ", look for task in error in logs.";
              break;
          }

          Logger?.LogDebug("Error handler for {taskId}, {taskStatus}: {details}",
                           resultStatusData.TaskId,
                           taskStatus,
                           details);
          try
          {
            errorHandler(resultStatusData.TaskId,
                         taskStatus,
                         details);
          }
          catch (Exception e)
          {
            Logger?.LogError(e,
                             "An error occured while handling a Task error {status}: {details}",
                             taskStatus,
                             details);
          }
        }

        missing.ExceptWith(resultStatusCollection.IdsResultError.Select(x => x.TaskId));

        foreach (var resultStatusData in resultStatusCollection.Canceled)
        {
          try
          {
            errorHandler(resultStatusData.TaskId,
                         TaskStatus.Unspecified,
                         "Task is missing");
          }
          catch (Exception e)
          {
            Logger?.LogError(e,
                             "An error occured while handling a Task error {status}: {details}",
                             TaskStatus.Unspecified,
                             "Task is missing");
          }
        }

        missing.ExceptWith(resultStatusCollection.Canceled.Select(x => x.TaskId));

        if (holdPrev == missing.Count)
        {
          idx = idx >= waitInSeconds.Count - 1
                  ? waitInSeconds.Count - 1
                  : idx                 + 1;

          Logger?.LogDebug("No Results are ready. Wait for {timeWait} seconds before new retry",
                           waitInSeconds[idx] / 1000);
        }
        else
        {
          idx = 0;
        }

        holdPrev = missing.Count;

        Thread.Sleep(waitInSeconds[idx]);
      }
    }
  }

  private void ResultTask()
  {
    while (!(CancellationResultTaskSource.Token.IsCancellationRequested && ResultHandlerDictionary.IsEmpty))
    {
      try
      {
        if (!ResultHandlerDictionary.IsEmpty)
        {
          ProxyTryGetResults(ResultHandlerDictionary.Keys.ToList(),
                             (taskId,
                              byteResult) =>
                             {
                               try
                               {
                                 var result = ProtoSerializer.DeSerializeMessageObjectArray(byteResult);
                                 ResultHandlerDictionary[taskId]
                                   .HandleResponse(result?[0],
                                                   taskId);
                               }
                               catch (Exception e)
                               {
                                 const ArmonikStatusCode statusCode = ArmonikStatusCode.Unknown;

                                 ServiceInvocationException ex;

                                 var ae = e as AggregateException;

                                 if (ae is not null && ae.InnerExceptions.Count > 1)
                                 {
                                   ex = new ServiceInvocationException(ae,
                                                                       statusCode);
                                 }
                                 else if (ae is not null)
                                 {
                                   ex = new ServiceInvocationException(ae.InnerException,
                                                                       statusCode);
                                 }
                                 else
                                 {
                                   ex = new ServiceInvocationException(e,
                                                                       statusCode);
                                 }

                                 ResultHandlerDictionary[taskId]
                                   .HandleError(ex,
                                                taskId);
                               }
                               finally
                               {
                                 ResultHandlerDictionary.TryRemove(taskId,
                                                                   out _);
                               }
                             },
                             (taskId,
                              taskStatus,
                              ex) =>
                             {
                               try
                               {
                                 var statusCode = StatusCodesLookUp.Keys.Contains(taskStatus)
                                                    ? StatusCodesLookUp[taskStatus]
                                                    : ArmonikStatusCode.Unknown;

                                 ResultHandlerDictionary[taskId]
                                   .HandleError(new ServiceInvocationException(ex,
                                                                               statusCode),
                                                taskId);
                               }
                               finally
                               {
                                 ResultHandlerDictionary.TryRemove(taskId,
                                                                   out _);
                               }
                             });
        }
        else
        {
          Thread.Sleep(100);
        }
      }
      catch (Exception e)
      {
        Logger?.LogError("An error occured while fetching results: {e}",
                         e);
      }
    }

    if (!ResultHandlerDictionary.IsEmpty)
    {
      Logger?.LogWarning("Results not processed : [{resultsNotProcessed}]",
                         string.Join(", ",
                                     ResultHandlerDictionary.Keys));
    }
  }


  /// <summary>
  ///   Get a new channel to communicate with the control plane
  /// </summary>
  /// <returns>gRPC channel</returns>
  public ChannelBase GetChannel()
    => SessionService.ChannelPool.GetChannel();

  /// <summary>Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.</summary>
  public override void Dispose()
  {
    CancellationResultTaskSource.Cancel();
    HandlerResponse?.Wait();
    HandlerResponse?.Dispose();

    SessionService        = null;
    SessionServiceFactory = null;
    semaphoreSlim_.Dispose();
  }

  /// <summary>
  ///   The method to destroy the service and close the session
  /// </summary>
  public void Destroy()
    => Dispose();

  /// <summary>
  ///   Check if this service has been destroyed before that call
  /// </summary>
  /// <returns>Returns true if the service was destroyed previously</returns>
  public bool IsDestroyed()
  {
    if (SessionService == null || SessionServiceFactory == null)
    {
      return true;
    }

    return false;
  }

  /// <summary>
  ///   Class to return TaskId and the result
  /// </summary>
  public class ServiceResult
  {
    /// <summary>
    ///   The getter to return the taskId
    /// </summary>
    public string TaskId { get; set; }

    /// <summary>
    ///   The getter to return the result in object type format
    /// </summary>
    public object Result { get; set; }
  }
}
