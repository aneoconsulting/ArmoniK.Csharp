﻿// This file is part of the ArmoniK project
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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.DevelopmentKit.Client.Exceptions;
using ArmoniK.DevelopmentKit.Client.Factory;
using ArmoniK.DevelopmentKit.Common;
using ArmoniK.DevelopmentKit.Common.Exceptions;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.DevelopmentKit.Client.Services.Common;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;


namespace ArmoniK.DevelopmentKit.Client.Services.Submitter
{
  /// <summary>
  /// This class is instantiated by ServiceFactory and allows to execute task on ArmoniK
  /// Grid.
  /// </summary>
  [MarkDownDoc]
  public class Service : AbstractClientService
  {
    /// <summary>
    /// Class to return TaskId and the result
    /// </summary>
    public class ServiceResult
    {
      /// <summary>
      /// The getter to return the taskId
      /// </summary>
      public TaskResultId TaskId { get; set; }

      /// <summary>
      /// The getter to return the result in object type format
      /// </summary>
      public object Result { get; set; }
    }

    /// <summary>
    /// Property Get the SessionId
    /// </summary>
    private SessionService SessionService { get; set; }

    private ILogger Logger { get; set; }

    private ProtoSerializer ProtoSerializer { get; }

    private SessionServiceFactory SessionServiceFactory { get; set; }

    private CancellationTokenSource CancellationTokenSource { get; set; }

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
    /// The default constructor to open connection with the control plane
    /// and create the session to ArmoniK
    /// </summary>
    /// <param name="properties">The properties containing TaskOptions and information to communicate with Control plane and </param>
    public Service(Properties properties) : base(properties)
    {
      SessionServiceFactory = new SessionServiceFactory(LoggerFactory);

      SessionService = SessionServiceFactory.CreateSession(properties);

      ProtoSerializer = new ProtoSerializer();

      CancellationTokenSource = new CancellationTokenSource();

      HandlerResponse = Task.Run(ResultTask,
                                 CancellationTokenSource.Token);

      Logger = LoggerFactory.CreateLogger<Service>();
      Logger.BeginPropertyScope(("SessionId", SessionService.SessionId),
                                ("Class", "Service"));
    }

    /// <summary>
    /// This function execute code locally with the same configuration as Armonik Grid execution
    /// The method needs the Service to execute, the method name to call and arguments of method to pass
    /// </summary>
    /// <param name="service">The instance of object containing the method to call</param>
    /// <param name="methodName">The string name of the method</param>
    /// <param name="arguments">the array of object to pass as arguments for the method</param>
    /// <returns>Returns an object as result of the method call</returns>
    /// <exception cref="WorkerApiException"></exception>
    [CanBeNull]
    public ServiceResult LocalExecute(object service, string methodName, object[] arguments)
    {
      var methodInfo = service.GetType().GetMethod(methodName);

      if (methodInfo == null)
        throw new InvalidOperationException($"MethodName [{methodName}] was not found");

      var result = methodInfo.Invoke(service,
                                     arguments);

      return new ServiceResult()
      {
        TaskId = new TaskResultId
        {
          SessionId = SessionId,
          TaskId = Guid.NewGuid().ToString(),
        },
        Result = result,
      };
    }

    /// <summary>
    /// This method is used to execute task and waiting after the result.
    /// the method will return the result of the execution until the grid returns the task result
    /// </summary>
    /// <param name="methodName">The string name of the method</param>
    /// <param name="arguments">the array of object to pass as arguments for the method</param>
    /// <returns>Returns a tuple with the taskId string and an object as result of the method call</returns>
    public ServiceResult Execute(string methodName, object[] arguments)
    {
      ArmonikPayload dataSynapsePayload = new()
      {
        ArmonikRequestType = ArmonikRequestType.Execute,
        MethodName         = methodName,
        ClientPayload      = ProtoSerializer.SerializeMessageObjectArray(arguments)
      };

      TaskResultId taskId = SessionService.SubmitTask(dataSynapsePayload.Serialize());

      var result = ProtoSerializer.DeSerializeMessageObjectArray(SessionService.GetResult(taskId));

      return new ServiceResult()
      {
        TaskId = taskId,
        Result = result?[0],
      };
    }

    /// <summary>
    /// This method is used to execute task and waiting after the result.
    /// the method will return the result of the execution until the grid returns the task result
    /// </summary>
    /// <param name="methodName">The string name of the method</param>
    /// <param name="dataArg">the array of byte to pass as argument for the methodName(byte[] dataArg)</param>
    /// <returns>Returns a tuple with the taskId string and an object as result of the method call</returns>
    public ServiceResult Execute(string methodName, byte[] dataArg)
    {
      ArmonikPayload dataSynapsePayload = new()
      {
        ArmonikRequestType  = ArmonikRequestType.Execute,
        MethodName          = methodName,
        ClientPayload       = dataArg,
        SerializedArguments = true,
      };

      TaskResultId taskId = null;
      object[]     result;

      try
      {
        taskId = SessionService.SubmitTask(dataSynapsePayload.Serialize());

        result = ProtoSerializer.DeSerializeMessageObjectArray(SessionService.GetResult(taskId));
      }
      catch (Exception e)
      {
        var status = SessionService.GetTaskStatus(taskId!.TaskId);

        var details = "";

        if (status != TaskStatus.Completed)
        {
          var output = SessionService.GetTaskOutputInfo(taskId.TaskId);
          details = output.TypeCase == Output.TypeOneofCase.Error ? output.Error.Details : "";
        }

        throw new ServiceInvocationException(e is AggregateException ? e.InnerException : e,
                                             StatusCodesLookUp.Keys.Contains(status) ? StatusCodesLookUp[status] : ArmonikStatusCode.Unknown)
        {
          OutputDetails = details,
        };
      }

      return new()
      {
        TaskId = taskId,
        Result = result?[0],
      };
    }

    /// <summary>
    /// The function submit where all information are already ready to send with class ArmonikPayload
    /// </summary>
    /// <param name="payload">Th armonikPayload to pass with Function name and serialized arguments</param>
    /// <param name="handler">The handler callBack for Error and response</param>
    /// <returns>Return the taskId</returns>
    public TaskResultId SubmitTask(ArmonikPayload payload, IServiceInvocationHandler handler)
    {
      return SubmitTasks(new[]
                         {
                           payload
                         },
                         handler).Single();
    }

    private void ProxyTryGetResults(IEnumerable<ResultIds> taskIds, Action<ResultIds, byte[]> responseHandler, Action<ResultIds, Exception> errorHandler)
    {
      var missing = taskIds.Select(id => new ResultIds
      {
        SessionId = id.SessionId,
        Ids       = id.Ids,
      }).ToHashSet();
      var results  = new List<Tuple<ResultIds, byte[]>>();
      var cts      = new CancellationTokenSource();
      var holdPrev = 0;
      var waitInSeconds = new List<int>
      {
        1000,
        5000,
        10000,
        20000,
        30000,
      };
      var       idx = 0;
      using var _   = Logger.BeginPropertyScope(("Function", "ActiveGetResults"));

      while (missing.Count != 0)
      {
        foreach (var bucket in missing.Batch(10000))
        {
          try
          {
            var partialResults = SessionService.TryGetResults(bucket);

            foreach (var (taskId, bytesArray) in partialResults)
            {
              responseHandler(taskId,
                              bytesArray);
            }

            results.AddRange(partialResults);

            missing.ExceptWith(partialResults.Select(x => x.Item1));


            if (holdPrev == results.Count)
            {
              idx = idx >= waitInSeconds.Count - 1 ? waitInSeconds.Count - 1 : idx + 1;
              Logger.LogInformation("No Result is ready. Wait for {timeWait} seconds before new retry",
                                    waitInSeconds[idx] / 1000);
            }
            else
            {
              idx = 0;
            }

            holdPrev = results.Count;

            Thread.Sleep(waitInSeconds[idx]);
          }
          catch (Exception e)
          {
            errorHandler(bucket.First(),
                         e);
            return;
          }
        }
      }

      cts.Cancel();
    }

    private void ResultTask()
    {
      while (!CancellationTokenSource.Token.IsCancellationRequested)
      {
        if (!ResultHandlerDictionary.IsEmpty)
        {
          try
          {
            ProxyTryGetResults(ResultHandlerDictionary.Keys.ToList(),
                               (taskId, byteResult) =>
                               {
                                 try
                                 {
                                   var result = ProtoSerializer.DeSerializeMessageObjectArray(byteResult);
                                   ResultHandlerDictionary[taskId].HandleResponse(result?[0],
                                                                                  taskId);
                                 }
                                 catch (Exception e)
                                 {

                                   // todo : look at the result of waitforavailability

                                   ServiceInvocationException ex = new(e is AggregateException ? e.InnerException : e,
                                                                       ArmonikStatusCode.Unknown)
                                   {
                                   };

                                   ResultHandlerDictionary[taskId].HandleError(ex,
                                                                               taskId);
                                 }

                                 ResultHandlerDictionary.TryRemove(taskId,
                                                                   value: out _);
                               },
                               (taskId, ex) =>
                               {
                                 switch (ex)
                                 {
                                   case ServiceInvocationException invocationException:
                                     ResultHandlerDictionary[taskId].HandleError(invocationException,
                                                                                 taskId);
                                     break;
                                   case AggregateException aggregateException:
                                     ResultHandlerDictionary[taskId].HandleError(new(aggregateException.InnerException,
                                                                                     ArmonikStatusCode.ResultError),
                                                                                 taskId);
                                     break;
                                   default:
                                     ResultHandlerDictionary[taskId].HandleError(new(ex,
                                                                                     ArmonikStatusCode.ResultError),
                                                                                 taskId);
                                     break;
                                 }

                                 ResultHandlerDictionary.TryRemove(taskId,
                                                                   value: out _);
                               });
          }
          catch (Exception e)
          {
            ServiceInvocationException ex = new(e is AggregateException ? e.InnerException : e,
                                                ArmonikStatusCode.ResultError);

            ResultHandlerDictionary.First().Value.HandleError(ex,
                                                              new ResultIds
                                                              {
                                                                Ids = new List<string>
                                                                {
                                                                  "AggregateListOfTaskId",
                                                                },
                                                              });
          }
        }
        else
        {
          Thread.Sleep(100);
        }
      }
    }


    /// <summary>
    /// The function submit where all information are already ready to send with class ArmonikPayload
    /// </summary>
    /// <param name="payloads">Th armonikPayload to pass with Function name and serialized arguments</param>
    /// <param name="handler">The handler callBack for Error and response</param>
    /// <returns>Return the taskId</returns>
    public IEnumerable<TaskResultId> SubmitTasks(IEnumerable<ArmonikPayload> payloads, IServiceInvocationHandler handler)
    {
      var taskIds       = SessionService.SubmitTasks(payloads.Select(p => p.Serialize()));
      var taskResultIds = taskIds as TaskResultId[] ?? taskIds.ToArray();
      foreach (var taskId in taskResultIds)
      {
        ResultHandlerDictionary[taskId] = handler;
      }

      return taskResultIds;
    }

    /// <summary>
    /// The method submit will execute task asynchronously on the server
    /// </summary>
    /// <param name="methodName">The name of the method inside the service</param>
    /// <param name="arguments">A list of object that can be passed in parameters of the function</param>
    /// <param name="handler">The handler callBack implemented as IServiceInvocationHandler to get response or result or error</param>
    /// <returns>Return the taskId string</returns>
    public TaskResultId Submit(string methodName, object[] arguments, IServiceInvocationHandler handler)
    {
      ArmonikPayload payload = new()
      {
        ArmonikRequestType = ArmonikRequestType.Execute,
        MethodName         = methodName,
        ClientPayload      = ProtoSerializer.SerializeMessageObjectArray(arguments)
      };

      return SubmitTasks(new[]
                         {
                           payload
                         },
                         handler).Single();
    }

    /// <summary>
    /// The method submit with One serialized argument that will be already serialized for byte[] MethodName(byte[] argument).
    /// </summary>
    /// <param name="methodName">The name of the method inside the service</param>
    /// <param name="argument">One serialized argument that will already serialize for MethodName.</param>
    /// <param name="handler">The handler callBack implemented as IServiceInvocationHandler to get response or result or error</param>
    /// <returns>Return the taskId string</returns>
    public IEnumerable<TaskResultId> Submit(string methodName, byte[] argument, IServiceInvocationHandler handler)
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
                         handler);
    }

    /// <summary>
    /// The method submit list of task with Enumerable list of arguments that will be serialized to each call of byte[] MethodName(byte[] argument)
    /// </summary>
    /// <param name="methodName">The name of the method inside the service</param>
    /// <param name="arguments">A list of parameters that can be passed in parameters of the each call of function</param>
    /// <param name="handler">The handler callBack implemented as IServiceInvocationHandler to get response or result or error</param>
    /// <returns>Return the list of created taskIds</returns>
    public IEnumerable<TaskResultId> Submit(string methodName, IEnumerable<object[]> arguments, IServiceInvocationHandler handler)
    {
      var armonikPayloads = arguments.Select(args => new ArmonikPayload()
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
    /// The method submit list of task with Enumerable list of serialized arguments that will be already serialized for byte[] MethodName(byte[] argument).
    /// </summary>
    /// <param name="methodName">The name of the method inside the service</param>
    /// <param name="arguments">List of serialized arguments that will already serialize for MethodName.</param>
    /// <param name="handler">The handler callBack implemented as IServiceInvocationHandler to get response or result or error</param>
    /// <returns>Return the taskId string</returns>
    public TaskResultId Submit(string methodName, IEnumerable<byte[]> arguments, IServiceInvocationHandler handler)
    {
      var armonikPayloads = arguments.Select(args => new ArmonikPayload()
      {
        ArmonikRequestType  = ArmonikRequestType.Execute,
        MethodName          = methodName,
        ClientPayload       = args,
        SerializedArguments = true,
      });

      return SubmitTasks(armonikPayloads,
                         handler).Single();
    }

    /// <summary>
    /// The handler to send the response
    /// </summary>
    public Task HandlerResponse { get; set; }

    /// <summary>
    /// The sessionId
    /// </summary>
    public string SessionId => SessionService?.SessionId.Id;

    /// <summary>Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.</summary>
    public override void Dispose()
    {
      CancellationTokenSource.Cancel();
      HandlerResponse?.Wait();
      HandlerResponse?.Dispose();

      SessionService        = null;
      SessionServiceFactory = null;
    }

    /// <summary>
    /// The method to destroy the service and close the session
    /// </summary>
    public void Destroy()
    {
      Dispose();
    }

    /// <summary>
    /// Check if this service has been destroyed before that call
    /// </summary>
    /// <returns>Returns true if the service was destroyed previously</returns>
    public bool IsDestroyed()
    {
      if (SessionService == null || SessionServiceFactory == null)
        return true;

      return false;
    }
  }
}