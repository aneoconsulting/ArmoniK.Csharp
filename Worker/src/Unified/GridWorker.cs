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
using System.Linq;
using System.Reflection;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.DevelopmentKit.Common;
using ArmoniK.DevelopmentKit.Common.Exceptions;
using ArmoniK.DevelopmentKit.Worker.Common;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

#pragma warning disable CS1591

// ReSharper disable once CheckNamespace

namespace ArmoniK.DevelopmentKit.Worker.Unified;

[XmlDocIgnore]
[PublicAPI]
public class GridWorker : IGridWorker
{
  private ServiceContext? serviceContext_;

  public GridWorker(IConfiguration configuration,
                    ILoggerFactory factory)
  {
    Configuration = configuration;
    LoggerFactory = factory;
    Logger        = factory.CreateLogger<GridWorker>();
  }

  private ILogger<GridWorker> Logger { get; }

  public ILoggerFactory LoggerFactory { get; set; }

  public IConfiguration Configuration { get; set; }

  public object? ServiceClass { get; set; }

  public string? GridServiceName { get; set; } = "BadServiceName";

  public string? GridAppNamespace { get; set; } = "BadServiceNamespace";

  public string? GridAppVersion { get; set; } = "BadAppsVersion";

  public string GridAppName { get; set; } = "BadAppsName";

  public TaskOptions? TaskOptions { get; set; }

  public IConfiguration? Configurations { get; set; }

  public void Configure(IConfiguration? configuration,
                        TaskOptions     clientOptions,
                        IAppsLoader     appsLoader)
  {
    Configurations = configuration;
    TaskOptions    = clientOptions.Clone();


    GridAppName      = clientOptions.ApplicationName;
    GridAppVersion   = clientOptions.ApplicationVersion;
    GridAppNamespace = clientOptions.ApplicationNamespace;
    GridServiceName  = clientOptions.ApplicationService;

    serviceContext_ = new ServiceContext
                      {
                        ApplicationName  = GridAppName,
                        ServiceName      = GridServiceName,
                        ClientLibVersion = GridAppVersion,
                        AppNamespace     = GridAppNamespace,
                      };

    ServiceClass = appsLoader.GetServiceContainerInstance<object>(GridAppNamespace,
                                                                  GridServiceName);

    if (ServiceClass is ITaskOptionsConfiguration iTaskOptionsConfiguration)
    {
      iTaskOptionsConfiguration.ConfigureTaskOptions(clientOptions);
    }

    if (ServiceClass is ILoggerConfiguration iLoggerConfiguration)
    {
      iLoggerConfiguration.ConfigureLogger(configuration);
    }
  }

  public void InitializeSessionWorker(Session     session,
                                      TaskOptions requestTaskOptions)
  {
    if (session == null)
    {
      throw new ArgumentNullException(nameof(session));
    }

    Logger.BeginPropertyScope(("sessionId", session));
  }

  public byte[]? Execute(ITaskHandler taskHandler)
  {
    using var _ = Logger.BeginPropertyScope(("sessionId", taskHandler.SessionId),
                                            ("taskId", $"{taskHandler.TaskId}"));

    var payload = taskHandler.Payload;

    var armonikPayload = ArmonikPayload.Deserialize(payload) ?? throw new NoNullAllowedException(nameof(payload));

    var methodName = armonikPayload.MethodName;
    if (methodName == null)
    {
      throw new WorkerApiException($"Method name is empty in Service class [{GridAppNamespace}.{GridServiceName}]");
    }


    var arguments = armonikPayload.SerializedArguments
                      ? new object?[]
                        {
                          armonikPayload.ClientPayload,
                        }
                      : ProtoSerializer.DeSerializeMessageObjectArray(armonikPayload.ClientPayload);

    MethodInfo methodInfo;
    if (arguments == null || arguments.Any() == false)
    {
      methodInfo = ServiceClass?.GetType()
                               .GetMethod(methodName) ?? throw new NoNullAllowedException(nameof(ServiceClass));
    }
    else
    {
      methodInfo = ServiceClass?.GetType()
                               .GetMethod(methodName,
                                          arguments.Select(x => x!.GetType())
                                                   .ToArray()) ?? throw new NoNullAllowedException(nameof(ServiceClass));
    }

    if (ServiceClass is ITaskSubmitterWorkerServiceConfiguration serviceContext)
    {
      var taskContext = new TaskContext
                        {
                          TaskId              = taskHandler.TaskId,
                          TaskInput           = taskHandler.Payload,
                          SessionId           = taskHandler.SessionId,
                          DependenciesTaskIds = taskHandler.DataDependencies.Select(t => t.Key),
                          DataDependencies    = taskHandler.DataDependencies,
                        };
      serviceContext.TaskContext = taskContext;
      serviceContext.ConfigureSessionService(taskHandler);
    }

    if (methodInfo == null)
    {
      throw new
        WorkerApiException($"Cannot found method [{methodName}({string.Join(", ", arguments?.Select(x => x?.GetType().Name) ?? new List<string>())})] in Service class [{GridAppNamespace}.{GridServiceName}]");
    }

    try
    {
      var result = methodInfo.Invoke(ServiceClass,
                                     arguments);
      if (result != null)
      {
        return new ProtoSerializer().SerializeMessageObjectArray(new[]
                                                                 {
                                                                   result,
                                                                 });
      }
    }
    // Catch all exceptions from MethodBase.Invoke except TargetInvocationException (triggered by an exception in the invoked code)
    // which we want to catch higher to allow for task retry
    catch (TargetException e)
    {
      throw new WorkerApiException(e);
    }
    catch (ArgumentException e)
    {
      throw new WorkerApiException(e);
    }
    catch (TargetParameterCountException e)
    {
      throw new WorkerApiException(e);
    }
    catch (MethodAccessException e)
    {
      throw new WorkerApiException(e);
    }
    catch (NotSupportedException e)
    {
      throw new WorkerApiException(e);
    }
    catch (TargetInvocationException e)
    {
      throw e.InnerException ?? e;
    }

    return null;
  }

  public void SessionFinalize()
  {
  }

  public void DestroyService()
    => Dispose();

  /// <summary>
  ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources
  /// </summary>
  public void Dispose()
    => SessionFinalize();
}
