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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.DevelopmentKit.Common;

using Google.Protobuf.WellKnownTypes;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace ArmoniK.DevelopmentKit.Worker.GridServer;

/// <summary>
///   The main object to communicate with the control Plane from the client side
///   The class will connect to the control plane to createSession, SubmitTask,
///   Wait for result and get the result.
///   See an example in the project ArmoniK.Samples in the sub project
///   https://github.com/aneoconsulting/ArmoniK.Samples/tree/main/Samples/GridServerLike
///   Samples.ArmoniK.Sample.SymphonyClient
/// </summary>
[MarkDownDoc]
public class ArmonikDataSynapsePollingService
{
  private readonly  IConfigurationSection                     controlPlanAddress_;
  internal readonly ILogger<ArmonikDataSynapsePollingService> Logger;

  /// <summary>
  ///   The ctor with IConfiguration and optional TaskOptions
  /// </summary>
  /// <param name="configuration">IConfiguration to set Client Data information and Grpc EndPoint</param>
  /// <param name="loggerFactory">The factory to create the logger for clientService</param>
  /// <param name="taskOptions">TaskOptions for any Session</param>
  public ArmonikDataSynapsePollingService(IConfiguration configuration,
                                          ILoggerFactory loggerFactory,
                                          TaskOptions    taskOptions = null)
  {
    controlPlanAddress_ = configuration.GetSection(SectionControlPlan);

    Logger = loggerFactory.CreateLogger<ArmonikDataSynapsePollingService>();
  }

  /// <summary>
  ///   Returns the section key Grpc from appSettings.json
  /// </summary>
  public static string SectionControlPlan { get; } = "Grpc";

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

  /// <summary>
  ///   This method is creating a default taskOptions initialization where
  ///   MaxDuration is 40 seconds, MaxRetries = 2 The app name is ArmoniK.DevelopmentKit.GridServer
  ///   The version is 1.0.0 the namespace ArmoniK.DevelopmentKit.GridServer and simple service FallBackServerAdder
  /// </summary>
  /// <returns>Return the default taskOptions</returns>
  public static TaskOptions InitializeDefaultTaskOptions()
    => new()
       {
         MaxDuration = new Duration
                       {
                         Seconds = 40,
                       },
         MaxRetries           = 2,
         Priority             = 1,
         EngineType           = EngineType.DataSynapse.ToString(),
         ApplicationName      = "ArmoniK.DevelopmentKit.Worker.GridServer",
         ApplicationVersion   = "1.0.0",
         ApplicationNamespace = "ArmoniK.DevelopmentKit.Worker.GridServer",
         ApplicationService   = "FallBackServerAdder",
       };


  /// <summary>
  ///   User method to submit task from the client
  ///   Need a client Service. In case of ServiceContainer
  ///   controlPlaneService can be null until the OpenSession is called
  /// </summary>
  /// <param name="payloads">
  ///   The user payload list to execute. General used for subTasking.
  /// </param>
  public IEnumerable<string> SubmitTasks(IEnumerable<byte[]> payloads)
    => throw new NotImplementedException("Polling agent service is not implemented for GridServer");

  /// <summary>
  ///   The method to submit several tasks with dependencies tasks. This task will wait for
  ///   to start until all dependencies are completed successfully
  /// </summary>
  /// <param name="session">The session Id where the task will be attached</param>
  /// <param name="payloadWithDependencies">A list of Tuple(taskId, Payload) in dependence of those created tasks</param>
  /// <returns>return a list of taskIds of the created tasks </returns>
  public IEnumerable<string> SubmitTasksWithDependencies(string                                    session,
                                                         IEnumerable<Tuple<byte[], IList<string>>> payloadWithDependencies)
    => throw new NotImplementedException("Polling agent service is not implemented for GridServer");


  /// <summary>
  ///   Try to find the result of One task. If there no result, the function return byte[0]
  /// </summary>
  /// <param name="taskId">The task Id trying to get result</param>
  /// <returns>Returns the result or byte[0] if there no result</returns>
  public byte[] TryGetResult(string taskId)
    => throw new NotImplementedException("Polling agent service is not implemented for GridServer");


  /// <summary>
  ///   Close Session. This function will disabled in nex Release. The session is automatically
  ///   closed after an other creation or after a disconnection or after end of timeout the tasks submitted
  /// </summary>
  public void CloseSession()
    => throw new NotImplementedException("Polling agent service is not implemented for GridServer");

  /// <summary>
  ///   Cancel the current Session where the SessionId is the one created previously
  /// </summary>
  public void CancelSession()
    => throw new NotImplementedException("Polling agent service is not implemented for GridServer");
}

/// <summary>
///   The ArmonikSymphonyClient Extension to single task creation
/// </summary>
public static class ArmonikDataSynapsePollingServiceExt
{
  /// <summary>
  ///   User method to submit task from the client
  /// </summary>
  /// <param name="client">The client instance for extension</param>
  /// <param name="payload">
  ///   The user payload to execute.
  /// </param>
  public static string SubmitTask(this ArmonikDataSynapsePollingService client,
                                  byte[]                                payload)
    => client.SubmitTasks(new[]
                          {
                            payload,
                          })
             .Single();

  /// <summary>
  ///   The method to submit One task with dependencies tasks. This task will wait for
  ///   to start until all dependencies are completed successfully
  /// </summary>
  /// <param name="client">The client instance for extension</param>
  /// <param name="payload">The payload to submit</param>
  /// <param name="dependencies">A list of task Id in dependence of this created task</param>
  /// <returns>return the taskId of the created task </returns>
  public static string SubmitTaskWithDependencies(this ArmonikDataSynapsePollingService client,
                                                  byte[]                                payload,
                                                  IList<string>                         dependencies)
    => throw new NotImplementedException("Polling agent service is not implemented for GridServer");

  /// <summary>
  ///   Get the result of One task. If there no result, the function return byte[0]
  /// </summary>
  /// <param name="client">The client instance for extension</param>
  /// <param name="taskId">The task Id trying to get result</param>
  /// <returns>Returns the result or byte[0] if there no result</returns>
  public static byte[] GetResult(this ArmonikDataSynapsePollingService client,
                                 string                                taskId)
    => throw new NotImplementedException("Polling agent service is not implemented for GridServer");
}
