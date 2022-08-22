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
using ArmoniK.DevelopmentKit.Common;

using Google.Protobuf;

using Microsoft.Extensions.Logging;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Api.Worker.Worker;

namespace ArmoniK.DevelopmentKit.SymphonyApi.api
{
  /// <summary>
  /// The class SessionService will be create each time the function CreateSession or OpenSession will
  /// be called by client or by the worker.
  /// </summary>
  [MarkDownDoc]
  public class SessionPollingService
  {
    /// <summary>
    ///   Set or Get TaskOptions with inside MaxDuration, Priority, AppName, VersionName and AppNamespace
    /// </summary>
    public TaskOptions TaskOptions { get; set; }

    /// <summary>
    ///   Only used for internal DO NOT USED IT
    ///   Get or Set SessionId object stored during the call of SubmitTask, SubmitSubTask,
    ///   SubmitSubTaskWithDependencies or WaitForCompletion, WaitForSubTaskCompletion or GetResults
    /// </summary>
    public Session SessionId { get; private set; }

    private ILoggerFactory LoggerFactory { get; set; }

    internal ILogger<SessionPollingService> Logger { get; set; }

    /// <summary>
    /// The task handler to communicate with the polling agent
    /// </summary>
    public ITaskHandler TaskHandler { get; set; }

    /// <summary>
    /// Ctor to instantiate a new SessionService
    /// This is an object to send task or get Results from a session
    /// </summary>
    public SessionPollingService(ILoggerFactory loggerFactory,
                                 ITaskHandler   taskHandler)
    {
      Logger        = loggerFactory.CreateLogger<SessionPollingService>();
      LoggerFactory = loggerFactory;
      TaskHandler   = taskHandler;

      TaskOptions = CopyClientToTaskOptions(TaskHandler.TaskOptions.Options);

      Logger.LogDebug("Creating Session... ");

      SessionId = new Session()
      {
        Id = TaskHandler.SessionId,
      };

      Logger.LogDebug($"Session Created {SessionId}");
    }


    /// <summary>Returns a string that represents the current object.</summary>
    /// <returns>A string that represents the current object.</returns>
    public override string ToString()
    {
      if (SessionId?.Id != null)
        return SessionId?.Id;
      else
        return "Session_Not_ready";
    }

    private static TaskOptions InitializeDefaultTaskOptions()
    {
      TaskOptions taskOptions = new()
      {
        MaxDuration = new()
        {
          Seconds = 300,
        },
        MaxRetries = 3,
        Priority   = 1,
      };
      taskOptions.Options.Add(AppsOptions.EngineTypeNameKey,
                              EngineType.Symphony.ToString());

      taskOptions.Options.Add(AppsOptions.GridAppNameKey,
                              "ArmoniK.Samples.SymphonyPackage");
      taskOptions.Options.Add(AppsOptions.GridAppVersionKey,
                              "1.0.0");
      taskOptions.Options.Add(AppsOptions.GridAppNamespaceKey,
                              "ArmoniK.Samples.Symphony.Packages");

      CopyTaskOptionsForClient(taskOptions);

      return taskOptions;
    }


    private static void CopyTaskOptionsForClient(TaskOptions taskOptions)
    {
      taskOptions.Options["MaxDuration"] = taskOptions.MaxDuration.Seconds.ToString();
      taskOptions.Options["MaxRetries"]  = taskOptions.MaxRetries.ToString();
      taskOptions.Options["Priority"]    = taskOptions.Priority.ToString();
    }

    private TaskOptions CopyClientToTaskOptions(IReadOnlyDictionary<string, string> clientOptions)
    {
      var defaultTaskOption = InitializeDefaultTaskOptions();

      TaskOptions taskOptions = new()
      {
        MaxDuration = new()
        {
          Seconds = clientOptions.ContainsKey("MaxDuration") ? long.Parse(clientOptions["MaxDuration"]) : defaultTaskOption.MaxDuration.Seconds,
        },
        MaxRetries = clientOptions.ContainsKey("MaxRetries") ? int.Parse(clientOptions["MaxRetries"]) : defaultTaskOption.MaxRetries,
        Priority   = clientOptions.ContainsKey("Priority") ? int.Parse(clientOptions["Priority"]) : defaultTaskOption.Priority,
      };

      defaultTaskOption.Options.ToList()
                       .ForEach(pair => taskOptions.Options[pair.Key] = pair.Value);

      clientOptions.ToList()
                   .ForEach(pair => taskOptions.Options[pair.Key] = pair.Value);

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
    public IEnumerable<TaskResultId> SubmitTasks(IEnumerable<byte[]> payloads)
    {
      using var _ = Logger.LogFunction();

      var ePayloads = payloads as byte[][] ?? payloads.ToArray();

      Logger.LogDebug("payload {len}",
                      ePayloads.Count());
      var taskRequests = new List<TaskRequest>();

      foreach (var payload in ePayloads)
      {
        var taskId = Guid.NewGuid().ToString();
        var taskRequest = new TaskRequest
        {
          Payload = ByteString.CopyFrom(payload),

          ExpectedOutputKeys =
          {
            taskId,
          },
        };

        taskRequests.Add(taskRequest);
      }

      var reply = TaskHandler.CreateTasksAsync(taskRequests,
                                               TaskOptions).Result;

      var taskCreated = reply.CreationStatusList.CreationStatuses.Select(status => new TaskResultId
      {
        ResultIds = status.TaskInfo.ExpectedOutputKeys,
        SessionId = SessionId.Id,
        TaskId    = status.TaskInfo.TaskId,
      });
      Logger.LogDebug("Tasks created : {ids}",
                      taskCreated.Select(id => id.TaskId));
      return taskCreated;
    }

    /// <summary>
    ///   The method to submit several tasks with dependencies tasks. This task will wait for
    ///   to start until all dependencies are completed successfully
    /// </summary>
    /// <param name="payloadsWithDependencies">A list of Tuple(taskId, Payload) in dependence of those created tasks</param>
    /// <param name="resultForParent"></param>
    /// <returns>return a list of taskIds of the created tasks </returns>
    public IEnumerable<TaskResultId> SubmitTasksWithDependencies(IEnumerable<Tuple<byte[], IList<string>>> payloadsWithDependencies, bool resultForParent = false)
    {
      using var _                = Logger.LogFunction();
      var       withDependencies = payloadsWithDependencies as Tuple<byte[], IList<string>>[] ?? payloadsWithDependencies.ToArray();
      Logger.LogDebug("payload with dependencies {len}",
                      withDependencies.Count());
      var taskRequests = new List<TaskRequest>();

      foreach (var (payload, dependencies) in withDependencies)
      {
        var expectedTaskId = resultForParent ? TaskHandler.TaskId : Guid.NewGuid().ToString();
        Logger.LogDebug("Expected task {expectedTaskId}",
                        expectedTaskId);
        var taskRequest = new TaskRequest
        {
          Payload = ByteString.CopyFrom(payload),

          ExpectedOutputKeys =
          {
            expectedTaskId,
          },
        };

        if (dependencies != null && dependencies.Count != 0)
        {
          taskRequest.DataDependencies.AddRange(dependencies);

          Logger.LogDebug("Dependencies : {dep}",
                          string.Join(", ",
                                      dependencies.Select(item => item.ToString())));
        }

        taskRequests.Add(taskRequest);
      }

      var reply = TaskHandler.CreateTasksAsync(taskRequests,
                                               TaskOptions).Result;

      var taskCreated = reply.CreationStatusList.CreationStatuses.Select(status => new TaskResultId
      {
        ResultIds = status.TaskInfo.ExpectedOutputKeys,
        SessionId = SessionId.Id,
        TaskId    = status.TaskInfo.TaskId,
      });
      Logger.LogDebug("Tasks created : {ids}",
                      taskCreated.Select(id => id.TaskId));
      return taskCreated;
    }


    /// <summary>
    /// 
    /// </summary>
    /// <returns></returns>
    public IDictionary<string, byte[]> GetDependenciesResults()
    {
      return TaskHandler.DataDependencies.ToDictionary(id => id.Key,
                                                       id => id.Value);
    }

    /// <summary>
    /// Get the dependencies data from previous executed and completed tasks
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
    public static TaskResultId SubmitTask(this SessionPollingService client, byte[] payload)
    {
      return client.SubmitTasks(new[] { payload })
                   .Single();
    }

    /// <summary>
    ///   The method to submit One task with dependencies tasks. This task will wait for
    ///   to start until all dependencies are completed successfully
    /// </summary>
    /// <param name="client">The client instance for extension</param>
    /// <param name="payload">The payload to submit</param>
    /// <param name="dependencies">A list of task Id in dependence of this created task</param>
    /// <returns>return the taskId of the created task </returns>
    public static TaskResultId SubmitTaskWithDependencies(this SessionPollingService client, byte[] payload, IList<string> dependencies)
    {
      return client.SubmitTasksWithDependencies(new[]
      {
        Tuple.Create(payload,
                     dependencies),
      }).Single();
    }
  }
}