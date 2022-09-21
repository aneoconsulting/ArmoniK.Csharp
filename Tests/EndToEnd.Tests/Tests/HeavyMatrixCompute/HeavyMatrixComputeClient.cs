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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.DevelopmentKit.Client.Exceptions;
using ArmoniK.DevelopmentKit.Client.Factory;
using ArmoniK.DevelopmentKit.Client.Services;
using ArmoniK.DevelopmentKit.Client.Services.Submitter;
using ArmoniK.DevelopmentKit.Common;
using ArmoniK.EndToEndTests.Common;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace ArmoniK.EndToEndTests.Tests.HeavyMatrixCompute;

public class HeavyMatrixComputeClient : ClientBaseTest<HeavyMatrixComputeClient>, IServiceInvocationHandler, IDisposable
{
  private int nbResults_;

  public HeavyMatrixComputeClient(IConfiguration configuration,
                                  ILoggerFactory loggerFactory)
    : base(configuration,
           loggerFactory)
  {
    Cts = new CancellationTokenSource();
  }

  public CancellationTokenSource Cts { get; set; }


  /// <summary>
  ///   The callBack method which has to be implemented to retrieve error or exception
  /// </summary>
  /// <param name="e">The exception sent to the client from the control plane</param>
  /// <param name="taskId">The task identifier which has invoke the error callBack</param>
  public void HandleError(ServiceInvocationException e,
                          string                     taskId)
  {
    Log.LogError($"Error from {taskId} : " + e.Message);
    throw new ApplicationException($"Error from {taskId}",
                                   e);
  }

  /// <summary>
  ///   The callBack method which has to be implemented to retrieve response from the server
  /// </summary>
  /// <param name="response">The object receive from the server as result the method called by the client</param>
  /// <param name="taskId">The task identifier which has invoke the response callBack</param>
  public void HandleResponse(object response,
                             string taskId)
  {
    switch (response)
    {
      case null:
        Log.LogInformation("Task finished but nothing returned in Result");
        break;
      case double value:
        nbResults_++;
        //Log.LogInformation($"Task {nbResults_++} finished with result {value}");
        break;
      case double[] doubles:
        //Log.LogInformation("Result is " + string.Join(", ",
        //                                              doubles));
        break;
      case byte[] values:
        Log.LogInformation("Result is " + string.Join(", ",
                                                      CheckUnifiedApi.SelectExtensions.ConvertToArray(values)));
        break;
    }
  }


  [EntryPoint]
  public override void EntryPoint()
  {
    Log.LogInformation("Configure taskOptions");
    var taskOptions = InitializeTaskOptions();
    OverrideTaskOptions(taskOptions);

    taskOptions.ApplicationService = "HeavyMatrixCompute";

    var props = new Properties(taskOptions,
                               Configuration.GetSection("Grpc")["EndPoint"],
                               5001);

    using var cs = ServiceFactory.CreateService(props,
                                                LoggerFactory, TimeSpan.FromMinutes(5));


    Log.LogInformation($"New session created : {cs.SessionId}");

    Log.LogInformation("Running End to End test to compute heavy matrix in sequential");


    ComputeMatrix(cs,
                  nbTasks: 16000,
                  nbElement: 64000); // 100 000 tasks x 30 KB of payload

    Cts = new CancellationTokenSource();
    //ComputeMatrix(cs,
    //              nbTasks: 100000,
    //              nbElement: 3840); // 100 000 tasks x 3 KB of payload

    //Cts = new CancellationTokenSource();
    //ComputeMatrix(cs,
    //              nbTasks: 1000,
    //              nbElement: 393216); // 1 000 tasks x 3 MB of payload
  }

  private static void OverrideTaskOptions(TaskOptions taskOptions)
    => taskOptions.EngineType = EngineType.Unified.ToString();


  private static object[] ParamsHelper(params object[] elements)
    => elements;

  private static void PeriodicInfo(Action            action,
                                   int               seconds,
                                   CancellationToken token = default)
  {
    if (action == null)
    {
      return;
    }

    Task.Run(async () =>
             {
               while (!token.IsCancellationRequested)
               {
                 action();
                 await Task.Delay(TimeSpan.FromSeconds(seconds),
                                  token);
               }
             },
             token);
  }

  /// <summary>
  ///   The first test developed to validate dependencies subTasking
  /// </summary>
  /// <param name="sessionService"></param>
  /// <param name="nbTasks">The number of task to submit</param>
  /// <param name="nbElement">The number of element n x M in the Matrix</param>
  private void ComputeMatrix(Service sessionService,
                             int     nbTasks,
                             int     nbElement)
  {
    var index_task = 0;
    var prev_index = 0;
    var elapse     = 30;
    nbResults_ = 0;
    var numbers = Enumerable.Range(0,
                                   nbElement)
                            .Select(x => (double)x)
                            .ToArray();
    Log.LogInformation($"===  Running from {nbTasks} tasks with payload by task {(nbElement * 8) / 1024} Ko Total : {(nbTasks * nbElement / 128) } Ko...   ===");

    PeriodicInfo(() =>
                 {
                   Log.LogInformation($"{index_task}/{nbTasks} Tasks. " + $"Got {nbResults_} results. " +
                                      $"Check Submission perf : Payload {(index_task - prev_index) * nbElement * 8.0 / 1024.0 / elapse:0.0} Ko/s, " +
                                      $"{(index_task - prev_index)                                                   / (double)elapse:0.00} tasks/s");
                   prev_index = index_task;

                 },
                 elapse,
                 Cts.Token);


    var sw = Stopwatch.StartNew();

    for (index_task = 0; index_task < nbTasks; index_task++)
    {
      Log.LogDebug($"{index_task}/{nbTasks} Task Time avg to submit {index_task / (sw.ElapsedMilliseconds / 1000.0):0.00} Task/s");

      sessionService.Submit("ComputeReduceCube",
                            ParamsHelper(numbers),
                            this);
    }

    Log.LogInformation($"{nbTasks} tasks executed in : {sw.ElapsedMilliseconds / 1000} secs with Total bytes {nbTasks * nbElement / 128} Ko");
    Cts.Cancel();
  }

  /// <summary>Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.</summary>
  public void Dispose()
  {
   
  }
}
