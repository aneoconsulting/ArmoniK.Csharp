// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using System.Linq;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.DevelopmentKit.Common;
using ArmoniK.DevelopmentKit.Common.Exceptions;
using ArmoniK.DevelopmentKit.Worker.Common;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Microsoft.Extensions.Logging;

namespace ArmoniK.DevelopmentKit.Worker.Unified;

/// <summary>
///   The class SessionService will be create each time the function CreateSession or OpenSession will
///   be called by client or by the worker.
/// </summary>
[MarkDownDoc]
public class SessionPollingService
{
  /// <summary>
  ///   Map between ids of task and their results id after task submission
  /// </summary>
  public readonly Dictionary<string, string> TaskId2OutputId;

  /// <summary>
  ///   Ctor to instantiate a new SessionService
  ///   This is an object to send task or get Results from a session
  /// </summary>
  public SessionPollingService(ILoggerFactory loggerFactory,
                               ITaskHandler   taskHandler)
  {
    Logger        = loggerFactory.CreateLogger<SessionPollingService>();
    LoggerFactory = loggerFactory;
    TaskHandler   = taskHandler;

    TaskOptions = TaskHandler.TaskOptions.Clone();

    Logger.LogDebug("Creating Session... ");

    SessionId = new Session
                {
                  Id = TaskHandler.SessionId,
                };

    Logger.LogDebug($"Session Created {SessionId}");

    TaskId2OutputId = new Dictionary<string, string>();
  }

  /// <summary>
  ///   Set or Get TaskOptions with inside MaxDuration, Priority, AppName, VersionName and AppNamespace
  /// </summary>
  public TaskOptions TaskOptions { get; set; }

  /// <summary>
  ///   Only used for internal DO NOT USED IT
  ///   Get or Set SessionId object stored during the call of SubmitTask, SubmitSubTask,
  ///   SubmitSubTaskWithDependencies or WaitForCompletion, WaitForSubTaskCompletion or GetResults
  /// </summary>
  public Session SessionId { get; }


  private ILoggerFactory LoggerFactory { get; }

  internal ILogger<SessionPollingService> Logger { get; set; }

  /// <summary>
  ///   The taskHandler to communicate with polling agent
  /// </summary>
  public ITaskHandler TaskHandler { get; set; }


  /// <summary>Returns a string that represents the current object.</summary>
  /// <returns>A string that represents the current object.</returns>
  public override string ToString()
    => SessionId?.Id ?? "Session_Not_ready";

  private static TaskOptions InitializeDefaultTaskOptions()
  {
    TaskOptions taskOptions = new()
                              {
                                MaxDuration = new Duration
                                              {
                                                Seconds = 300,
                                              },
                                MaxRetries           = 3,
                                Priority             = 1,
                                EngineType           = EngineType.Unified.ToString(),
                                ApplicationName      = "ArmoniK.Samples.UnifiedPackage",
                                ApplicationVersion   = "1.0.0",
                                ApplicationNamespace = "ArmoniK.Samples.Unified.Packages",
                              };

    return taskOptions;
  }

  /// <summary>
  ///   User method to submit task from the client
  ///   Need a client Service. In case of ServiceContainer
  ///   pollingAgentService can be null until the OpenSession is called
  /// </summary>
  /// <param name="payloads">
  ///   The user payload list to execute. General used for subTasking.
  /// </param>
  /// <param name="maxRetries">The number of retry before fail to submit task. Default = 5 retries</param>
  /// <param name="taskOptions">
  ///   TaskOptions argument to override default taskOptions in Session.
  ///   If non null it will override the default taskOptions in SessionService for client or given by taskHandler for worker
  /// </param>
  public IEnumerable<string> SubmitTasks(IEnumerable<byte[]> payloads,
                                         int                 maxRetries  = 5,
                                         TaskOptions         taskOptions = null)
  {
    using var _ = Logger.LogFunction();

    var resultIds = payloads.TryGetNonEnumeratedCount(out var count)
                      ? new ResultIdEnumerable(TaskHandler,
                                               count,
                                               true)
                      : new ResultIdEnumerable(TaskHandler);

    var taskRequests = payloads.Zip(resultIds,
                                    (bytes,
                                     resultId) => new TaskRequest
                                                  {
                                                    Payload = UnsafeByteOperations.UnsafeWrap(bytes.AsMemory(0,
                                                                                                             bytes.Length)),

                                                    ExpectedOutputKeys =
                                                    {
                                                      resultId,
                                                    },
                                                  });

    var createTaskReply = TaskHandler.CreateTasksAsync(taskRequests,
                                                       taskOptions ?? TaskOptions)
                                     .Result;

    switch (createTaskReply.ResponseCase)
    {
      case CreateTaskReply.ResponseOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateTaskReply.ResponseOneofCase.CreationStatusList:
        foreach (var creationStatus in createTaskReply.CreationStatusList.CreationStatuses)
        {
          TaskId2OutputId.Add(creationStatus.TaskInfo.TaskId,
                              creationStatus.TaskInfo.ExpectedOutputKeys.Single());
        }

        return createTaskReply.CreationStatusList.CreationStatuses.Select(status => status.TaskInfo.TaskId);
      case CreateTaskReply.ResponseOneofCase.Error:
        throw new Exception("Error while creating tasks !");
      default:
        throw new ArgumentOutOfRangeException();
    }
  }


  /// <summary>
  ///   The method to submit several tasks with dependencies tasks. This task will wait for
  ///   to start until all dependencies are completed successfully
  /// </summary>
  /// <param name="payloadsWithDependencies">A list of Tuple(taskId, Payload) in dependence of those created tasks</param>
  /// <param name="resultForParent"></param>
  /// <param name="maxRetries">The number of retry before fail to submit task. Default = 5 retries</param>
  /// <param name="taskOptions">
  ///   TaskOptions argument to override default taskOptions in Session.
  ///   If non null it will override the default taskOptions in SessionService for client or given by taskHandler for worker
  /// </param>
  /// <returns>return a list of taskIds of the created tasks </returns>
  public IEnumerable<string> SubmitTasksWithDependencies(IEnumerable<Tuple<byte[], IList<string>>> payloadsWithDependencies,
                                                         bool                                      resultForParent = false,
                                                         int                                       maxRetries      = 5,
                                                         TaskOptions                               taskOptions     = null)
  {
    using var _            = Logger.LogFunction();
    var       taskRequests = new List<TaskRequest>();

    foreach (var (payload, dependencies) in payloadsWithDependencies)
    {
      var taskRequest = new TaskRequest
                        {
                          Payload = UnsafeByteOperations.UnsafeWrap(payload.AsMemory(0,
                                                                                     payload.Length)),
                        };

      if (resultForParent)
      {
        taskRequest.ExpectedOutputKeys.AddRange(TaskHandler.ExpectedResults);
      }

      if (dependencies != null && dependencies.Count != 0)
      {
        foreach (var dependency in dependencies)
        {
          if (!TaskId2OutputId.TryGetValue(dependency,
                                           out var resultId))
          {
            throw new WorkerApiException($"Dependency {dependency} has no corresponding result id.");
          }

          taskRequest.DataDependencies.Add(resultId);
        }

        Logger.LogDebug("Dependencies : {dep}",
                        string.Join(", ",
                                    dependencies.Select(item => item.ToString())));
      }

      taskRequests.Add(taskRequest);
    }

    if (!resultForParent)
    {
      foreach (var (taskRequest, resultId) in taskRequests.Zip(new ResultIdEnumerable(TaskHandler,
                                                                                      taskRequests.Count,
                                                                                      true)))
      {
        taskRequest.ExpectedOutputKeys.Add(resultId);
      }
    }


    var createTaskReply = TaskHandler.CreateTasksAsync(taskRequests,
                                                       taskOptions ?? TaskOptions)
                                     .Result;

    switch (createTaskReply.ResponseCase)
    {
      case CreateTaskReply.ResponseOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateTaskReply.ResponseOneofCase.CreationStatusList:
        foreach (var creationStatus in createTaskReply.CreationStatusList.CreationStatuses)
        {
          TaskId2OutputId.Add(creationStatus.TaskInfo.TaskId,
                              creationStatus.TaskInfo.ExpectedOutputKeys.Single());
        }

        return createTaskReply.CreationStatusList.CreationStatuses.Select(status => status.TaskInfo.TaskId);
      case CreateTaskReply.ResponseOneofCase.Error:
        throw new Exception("Error while creating tasks !");
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  /// <summary>
  /// </summary>
  /// <returns></returns>
  public IDictionary<string, byte[]> GetDependenciesResults()
    => TaskHandler.DataDependencies.ToDictionary(id => id.Key,
                                                 id => id.Value);

  /// <summary>
  ///   Get the dependencies data from previous executed and completed tasks
  /// </summary>
  /// <returns>returns a specific data from the taskId </returns>
  public byte[] GetDependenciesResult(string id)
  {
    var isOkay = TaskHandler.DataDependencies.TryGetValue(id,
                                                          out var data);
    if (!isOkay)
    {
      throw new KeyNotFoundException(id);
    }

    return data;
  }
}

/// <summary>
///   The SessionService Extension to single task creation
/// </summary>
public static class SessionServiceExt
{
  /// <summary>
  ///   User method to submit task from the client
  /// </summary>
  /// <param name="client">The client instance for extension</param>
  /// <param name="payload">
  ///   The user payload to execute.
  /// </param>
  /// <param name="maxRetries">The number of retry before fail to submit task. Default = 5 retries</param>
  /// <param name="taskOptions">
  ///   TaskOptions argument to override default taskOptions in Session.
  ///   If non null it will override the default taskOptions in SessionService for client or given by taskHandler for worker
  /// </param>
  public static string SubmitTask(this SessionPollingService client,
                                  byte[]                     payload,
                                  int                        maxRetries  = 5,
                                  TaskOptions                taskOptions = null)
    => client.SubmitTasks(new[]
                          {
                            payload,
                          },
                          maxRetries,
                          taskOptions)
             .Single();

  /// <summary>
  ///   The method to submit One task with dependencies tasks. This task will wait for
  ///   to start until all dependencies are completed successfully
  /// </summary>
  /// <param name="client">The client instance for extension</param>
  /// <param name="payload">The payload to submit</param>
  /// <param name="dependencies">A list of task Id in dependence of this created task</param>
  /// <param name="maxRetries">The number of retry before fail to submit task. Default = 5 retries</param>
  /// <param name="taskOptions">
  ///   TaskOptions argument to override default taskOptions in Session.
  ///   If non null it will override the default taskOptions in SessionService for client or given by taskHandler for worker
  /// </param>
  /// <returns>return the taskId of the created task </returns>
  public static string SubmitTaskWithDependencies(this SessionPollingService client,
                                                  byte[]                     payload,
                                                  IList<string>              dependencies,
                                                  int                        maxRetries  = 5,
                                                  TaskOptions                taskOptions = null)
    => client.SubmitTasksWithDependencies(new[]
                                          {
                                            Tuple.Create(payload,
                                                         dependencies),
                                          },
                                          maxRetries: maxRetries,
                                          taskOptions: taskOptions)
             .Single();
}
