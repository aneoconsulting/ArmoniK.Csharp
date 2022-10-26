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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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
  // *** you need some mechanism to map types to fields
  private static readonly IDictionary<TaskStatus, ArmonikStatusCode> StatusCodesLookUp = new List<Tuple<TaskStatus, ArmonikStatusCode>>
                                                                                         {
                                                                                           Tuple.Create(TaskStatus.Submitted,
                                                                                                        ArmonikStatusCode.ResultNotReady),
                                                                                           Tuple.Create(TaskStatus.Timeout,
                                                                                                        ArmonikStatusCode.TaskTimeout),
                                                                                           Tuple.Create(TaskStatus.Canceled,
                                                                                                        ArmonikStatusCode.TaskCanceled),
                                                                                           Tuple.Create(TaskStatus.Canceling,
                                                                                                        ArmonikStatusCode.TaskCanceled),
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
    SessionServiceFactory = new SessionServiceFactory(LoggerFactory);

    SessionService = SessionServiceFactory.CreateSession(properties);

    ProtoSerializer = new ProtoSerializer();

    CancellationResultTaskSource = new CancellationTokenSource();

    HandlerResponse = Task.Run(ResultTask,
                               CancellationResultTaskSource.Token);

    Logger = LoggerFactory?.CreateLogger<Service>();
    Logger?.BeginPropertyScope(("SessionId", SessionService.SessionId),
                               ("Class", "Service"));
  }

  /// <summary>
  ///   Property Get the SessionId
  /// </summary>
  private SessionService SessionService { get; set; }

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

  /// <inheritdoc />
  public string Submit(string                    methodName,
                       object[]                  arguments,
                       IServiceInvocationHandler handler)
  {
    ArmonikPayload payload = new()
                             {
                               ArmonikRequestType = ArmonikRequestType.Execute,
                               MethodName         = methodName,
                               ClientPayload      = ProtoSerializer.SerializeMessageObjectArray(arguments),
                             };

    return SubmitTasks(new[]
                       {
                         payload,
                       },
                       handler)
      .Single();
  }

  /// <inheritdoc />
  public IEnumerable<string> Submit(string                    methodName,
                                    IEnumerable<object[]>     arguments,
                                    IServiceInvocationHandler handler)
  {
    var armonikPayloads = arguments.Select(args => new ArmonikPayload
                                                   {
                                                     ArmonikRequestType  = ArmonikRequestType.Execute,
                                                     MethodName          = methodName,
                                                     ClientPayload       = ProtoSerializer.SerializeMessageObjectArray(args),
                                                     SerializedArguments = false,
                                                   });


    return SubmitTasks(armonikPayloads,
                       handler);
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
    ArmonikPayload dataSynapsePayload = new()
                                        {
                                          ArmonikRequestType = ArmonikRequestType.Execute,
                                          MethodName         = methodName,
                                          ClientPayload      = ProtoSerializer.SerializeMessageObjectArray(arguments),
                                        };

    var taskId = SessionService.SubmitTask(dataSynapsePayload.Serialize());

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
    ArmonikPayload dataSynapsePayload = new()
                                        {
                                          ArmonikRequestType  = ArmonikRequestType.Execute,
                                          MethodName          = methodName,
                                          ClientPayload       = dataArg,
                                          SerializedArguments = true,
                                        };

    var      taskId = "not-TaskId";
    object[] result;

    try
    {
      taskId = SessionService.SubmitTask(dataSynapsePayload.Serialize());

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
  ///   The function submit where all information are already ready to send with class ArmonikPayload
  /// </summary>
  /// <param name="payload">Th armonikPayload to pass with Function name and serialized arguments</param>
  /// <param name="handler">The handler callBack for Error and response</param>
  /// <returns>Return the taskId</returns>
  public string SubmitTask(ArmonikPayload            payload,
                           IServiceInvocationHandler handler)
    => SubmitTasks(new[]
                   {
                     payload,
                   },
                   handler)
      .Single();

  private void ResultTask()
  {
    var waitInSeconds = new[]
                        {
                          10,
                          1000,
                          5000,
                          10000,
                          20000,
                          30000,
                        };
    var emptyFetch = 0;

    while (!(CancellationResultTaskSource.Token.IsCancellationRequested && ResultHandlerDictionary.IsEmpty))
    {
      try
      {
        if (ResultHandlerDictionary.IsEmpty)
        {
          Thread.Sleep(100);
          continue;
        }

        var fetched = 0;

        foreach (var chunk in ResultHandlerDictionary.Keys.ToChunk(500))
        {
          var resultStatusCollection = SessionService.GetResultStatus(chunk);

          foreach (var resultStatusData in resultStatusCollection.IdsReady)
          {
            var taskId = resultStatusData.TaskId;
            fetched += 1;

            try
            {
              var byteResult = SessionService.TryGetResultAsync(new ResultRequest
                                                                {
                                                                  ResultId = resultStatusData.ResultId,
                                                                  Session  = SessionId,
                                                                },
                                                                CancellationToken.None)
                                             .Result;
              Logger?.LogTrace("Response handler for {taskId}",
                               resultStatusData.TaskId);

              var result = ProtoSerializer.DeSerializeMessageObjectArray(byteResult);
              ResultHandlerDictionary[taskId]
                .HandleResponse(result?[0],
                                taskId);
            }
            catch (Exception e)
            {
              Logger?.LogDebug(e,
                               "Response handler for {taskId} threw an exception, calling error handler",
                               resultStatusData.TaskId);

              var ex = e;

              if (e is AggregateException ae)
              {
                ex = ae.InnerExceptions.Count > 1
                       ? ae
                       : ae.InnerException;
              }

              try
              {
                ResultHandlerDictionary[taskId]
                  .HandleError(new ServiceInvocationException(ex,
                                                              ArmonikStatusCode.Unknown),
                               taskId);
              }
              catch (Exception e2)
              {
                Logger?.LogError(e2,
                                 "An exception was thrown while handling a previous exception in the result handler of {taskId}",
                                 taskId);
              }
            }
            finally
            {
              ResultHandlerDictionary.TryRemove(taskId,
                                                out _);
            }
          }

          foreach (var resultStatusData in resultStatusCollection.IdsResultError)
          {
            var taskId     = resultStatusData.TaskId;
            var details    = "";
            var taskStatus = TaskStatus.Unspecified;
            fetched += 1;

            try
            {
              taskStatus = SessionService.GetTaskStatus(resultStatusData.TaskId);
              if (!StatusCodesLookUp.TryGetValue(taskStatus,
                                                 out var statusCode))
              {
                statusCode = ArmonikStatusCode.Unknown;
              }

              switch (taskStatus)
              {
                case TaskStatus.Canceling:
                case TaskStatus.Canceled:
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
              ResultHandlerDictionary[resultStatusData.TaskId]
                .HandleError(new ServiceInvocationException(details,
                                                            statusCode),
                             resultStatusData.TaskId);
            }
            catch (Exception e)
            {
              Logger?.LogError(e,
                               "An error occured while handling a Task error {status}: {details}",
                               taskStatus,
                               details);
            }
            finally
            {
              ResultHandlerDictionary.TryRemove(taskId,
                                                out _);
            }
          }
        }

        Thread.Sleep(waitInSeconds[emptyFetch]);
        emptyFetch = fetched == 0
                       ? Math.Min(emptyFetch + 1,
                                  waitInSeconds.Length)
                       : 0;
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
  ///   The function submit where all information are already ready to send with class ArmonikPayload
  /// </summary>
  /// <param name="payloads">Th armonikPayload to pass with Function name and serialized arguments</param>
  /// <param name="handler">The handler callBack for Error and response</param>
  /// <returns>Return the taskId</returns>
  public IEnumerable<string> SubmitTasks(IEnumerable<ArmonikPayload> payloads,
                                         IServiceInvocationHandler   handler)
  {
    var taskIds       = SessionService.SubmitTasks(payloads.Select(p => p.Serialize()));
    var submitTaskIds = taskIds as string[] ?? taskIds.ToArray();

    foreach (var taskId in submitTaskIds)
    {
      ResultHandlerDictionary[taskId] = handler;
    }

    return submitTaskIds;
  }

  /// <summary>
  ///   The method submit with One serialized argument that will be already serialized for byte[] MethodName(byte[]
  ///   argument).
  /// </summary>
  /// <param name="methodName">The name of the method inside the service</param>
  /// <param name="argument">One serialized argument that will already serialize for MethodName.</param>
  /// <param name="handler">The handler callBack implemented as IServiceInvocationHandler to get response or result or error</param>
  /// <returns>Return the taskId string</returns>
  public string Submit(string                    methodName,
                       byte[]                    argument,
                       IServiceInvocationHandler handler)
  {
    ArmonikPayload payload = new()
                             {
                               ArmonikRequestType  = ArmonikRequestType.Execute,
                               MethodName          = methodName,
                               ClientPayload       = argument,
                               SerializedArguments = true,
                             };

    return SubmitTasks(new[]
                       {
                         payload,
                       },
                       handler)
      .Single();
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
                                                     ArmonikRequestType  = ArmonikRequestType.Execute,
                                                     MethodName          = methodName,
                                                     ClientPayload       = args,
                                                     SerializedArguments = true,
                                                   });

    return SubmitTasks(armonikPayloads,
                       handler);
  }

  /// <summary>Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.</summary>
  public override void Dispose()
  {
    CancellationResultTaskSource.Cancel();
    HandlerResponse?.Wait();
    HandlerResponse?.Dispose();

    SessionService        = null;
    SessionServiceFactory = null;
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
