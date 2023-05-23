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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.DevelopmentKit.Client.Common;
using ArmoniK.DevelopmentKit.Client.Common.Exceptions;
using ArmoniK.DevelopmentKit.Client.Unified.Factory;
using ArmoniK.DevelopmentKit.Client.Unified.Services.Submitter;
using ArmoniK.DevelopmentKit.Common;
using ArmoniK.DevelopmentKit.Common.Extensions;
using ArmoniK.EndToEndTests.Client.Tests.CheckTypeOfSubmission;
using ArmoniK.EndToEndTests.Common;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace ArmoniK.EndToEndTests.Client.Tests.LargeSubmitAsync;

/// <summary>
///   The client submission class
/// </summary>
public class LargeSubmitAsyncClient : ClientBaseTest<LargeSubmitAsyncClient>, IServiceInvocationHandler
{
  /// <summary>
  ///   The ctor
  /// </summary>
  /// <param name="configuration"></param>
  /// <param name="loggerFactory"></param>
  public LargeSubmitAsyncClient(IConfiguration configuration,
                                ILoggerFactory loggerFactory)
    : base(configuration,
           loggerFactory)
  {
  }


  /// <summary>
  ///   Number of result received
  /// </summary>
  private int NbResults { get; set; }

  /// <summary>
  ///   The sum of avery result in once value
  /// </summary>
  private double Total { get; set; }

  private Properties Props { get; set; }

  /// <summary>
  ///   The callBack method which has to be implemented to retrieve error or exception
  /// </summary>
  /// <param name="e">The exception sent to the client from the control plane</param>
  /// <param name="taskId">The task identifier which has invoke the error callBack</param>
  public void HandleError(ServiceInvocationException e,
                          string                     taskId)
  {
    Log.LogError($"Error from {taskId} : " + e.Message);
    NbResults++;
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
        NbResults++;
        Total += value;
        break;
      case byte[] values:
        Log.LogInformation("Result is " + string.Join(", ",
                                                      values.ConvertToArray()));
        break;
    }
  }


  /// <summary>
  ///   Main ethod called by tests engine
  /// </summary>
  [EntryPoint]
  public override void EntryPoint()
  {
    Log.LogInformation("Configure taskOptions");
    var taskOptions = InitializeTaskOptions();
    OverrideTaskOptions(taskOptions);

    taskOptions.MaxRetries = 1;

    Props = new Properties(Configuration,
                           taskOptions,
                           Configuration.GetSection("Grpc")["EndPoint"],
                           5001)
            {
              MaxConcurrentBuffers = 5,
              MaxTasksPerBuffer    = 100,
              MaxParallelChannels  = 5,
              TimeTriggerBuffer    = TimeSpan.FromSeconds(10),
            };

    CompareSubmitPerfs(1000,
                       64000);
  }

  private void CompareSubmitPerfs(int nbTasks,
                                  int nbElement,
                                  int workloadTimeInMs = 1)
  {
    var service = ServiceFactory.CreateService(Props,
                                               LoggerFactory);

    Log.LogInformation($"New session created : {service.SessionId}");

    Log.LogInformation("Running End to End test to compute heavy vector in sequential");

    using var cancellationTokenSource = new CancellationTokenSource();
    var       prevIndex               = 0;
    const int elapsed                 = 20;
    NbResults = 0;

    var numbers = Enumerable.Range(0,
                                   nbElement)
                            .Select(_ => 42.0 / nbElement / nbTasks)
                            .ToArray();


    Log.LogInformation($"=== CompareSubmitPerfs Running Sequential from {nbTasks} tasks with payload by task {nbElement * 8 / 1024.0:0.00} Ko Total : {nbTasks * (nbElement / 128)} Ko...   ===");


    PeriodicInfo(() =>
                 {
                   Log.LogInformation($"Got {NbResults} results. {(NbResults - prevIndex) / (double)elapsed:0.00} results/s");
                   prevIndex = NbResults;
                 },
                 elapsed,
                 cancellationTokenSource.Token);

    var sw = Stopwatch.StartNew();

    var taskIds = ExecuteSubmitAsync(nbTasks,
                                     service,
                                     numbers,
                                     workloadTimeInMs,
                                     cancellationTokenSource.Token)
      .ToList();

    Log.LogInformation("Waiting for end of submission...");
    Log.LogInformation($"{taskIds.Count()}/{nbTasks} Async tasks submitted in : {sw.ElapsedMilliseconds / 1000.0:0.00} secs ({taskIds.Count * 1000 / sw.ElapsedMilliseconds:0.00} Tasks/s, {taskIds.Count * nbElement * 8.0 / 1024.0 / (sw.ElapsedMilliseconds / 1000.0):0.00} KB/s)");

    Assert.AreEqual(nbTasks,
                    taskIds.ToHashSet()
                           .Count);

    Log.LogDebug($"TaskIds : \n{string.Join("\n\t", taskIds)}");

    Log.LogInformation("Waiting for result before exit");

    service.Dispose();
    Log.LogInformation("Nb result received : {res} : total is {Total} in {time:0.00}",
                       NbResults,
                       (int)Total,
                       sw.ElapsedMilliseconds / 1000.0);

    Assert.AreEqual(nbTasks,
                    NbResults);
  }

  private IEnumerable<string> ExecuteSubmitAsync(int                 nbTasks,
                                                 Service             service,
                                                 IEnumerable<double> numbers,
                                                 int                 workloadTimeInMs,
                                                 CancellationToken   token = default)
  {
    var resultQueries = Enumerable.Range(0,
                                         nbTasks)
                                  .Batch(nbTasks / Props.MaxParallelChannels)
                                  .AsParallel();

    var resultTask = new ConcurrentBag<Task<string>>();
    var results    = new ConcurrentBag<string>();

    resultQueries.ForAll(bucket =>
                         {
                           var tasksInBucket = bucket.Select(async _ =>
                                                             {
                                                               var result = await service.SubmitAsync("ComputeSum",
                                                                                                      ParamsHelper(numbers,
                                                                                                                   workloadTimeInMs),
                                                                                                      this,
                                                                                                      token);

                                                               return result;
                                                             });

                           foreach (var task in tasksInBucket)
                           {
                             resultTask.Add(task);
                           }
                         });

    //Need to fix aync issue for a performance submission and check all exception one by one
    resultTask.AsParallel()
              .ForAll(task =>
                      {
                        if (task.IsFaulted)
                        {
                          if (task.Exception != null)
                          {
                            throw task.Exception;
                          }
                        }

                        results.Add(task.Result);
                      });

    return results.ToList();
  }

  private static void OverrideTaskOptions(TaskOptions taskOptions)
  {
    taskOptions.EngineType           = EngineType.Unified.ToString();
    taskOptions.ApplicationNamespace = "ArmoniK.EndToEndTests.Worker.Tests.LargePayloadSubmit";
    taskOptions.ApplicationService   = "LargePayloadSubmitWorker";
  }


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
}
