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
using System.Linq;
using System.Reflection;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.DevelopmentKit.Common;
using ArmoniK.DevelopmentKit.Common.Exceptions;
using ArmoniK.DevelopmentKit.Worker.Common;

using Google.Protobuf.WellKnownTypes;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

#pragma warning disable CS1591

namespace ArmoniK.DevelopmentKit.Worker.GridServer;

[XmlDocIgnore]
public class GridWorker : IGridWorker
{
  public GridWorker(IConfiguration? configuration,
                    ILoggerFactory? factory)
  {
    Configuration = configuration;
    LoggerFactory = factory;
    Logger        = factory?.CreateLogger<GridWorker>();
  }

  private ILogger<GridWorker>? Logger { get; }

  public ILoggerFactory? LoggerFactory { get; set; }

  public IConfiguration? Configuration { get; set; }

  public object? ServiceClass { get; set; }

  public string? GridServiceName { get; set; }

  public string? GridAppNamespace { get; set; }

  public string? GridAppVersion { get; set; }

  public string? GridAppName { get; set; }

  public TaskOptions? TaskOptions { get; set; }

  public IConfiguration? Configurations { get; set; }

  public ServiceAdminWorker? ServiceAdminWorker { get; set; }

  public ServiceInvocationContext? ServiceInvocationContext { get; set; }

  public void Configure(IConfiguration? configuration,
                        TaskOptions?    clientOptions,
                        IAppsLoader     appsLoader)
  {
    Configurations = configuration;
    TaskOptions    = clientOptions;


    GridAppName      = clientOptions?.ApplicationName;
    GridAppVersion   = clientOptions?.ApplicationVersion;
    GridAppNamespace = clientOptions?.ApplicationNamespace;
    GridServiceName  = clientOptions?.ApplicationService;

    ServiceClass = appsLoader.GetServiceContainerInstance<object>(GridAppNamespace,
                                                                  GridServiceName);
  }

  public void InitializeSessionWorker(Session     session,
                                      TaskOptions requestTaskOptions)
  {
    if (session == null)
    {
      throw new ArgumentNullException(nameof(session),
                                      "Session is null in the Execute function");
    }

    ServiceInvocationContext ??= new ServiceInvocationContext
                                 {
                                   SessionId = session,
                                 };
    Logger?.BeginPropertyScope(("SessionId", session));

    TaskOptions serviceAdminTaskOptions = new()
                                          {
                                            MaxDuration = new Duration
                                                          {
                                                            Seconds = 3600,
                                                          },
                                            MaxRetries = 5,
                                            Priority   = 1,
                                          };
  }


  public byte[] Execute(ITaskHandler taskHandler)
  {
    using var _ = Logger?.BeginPropertyScope(("SessionId", taskHandler.SessionId),
                                             ("TaskId", $"{taskHandler.TaskId}"));

    var payload = taskHandler.Payload;

    var dataSynapsePayload = ArmonikPayload.Deserialize(payload);

    if (dataSynapsePayload?.ArmonikRequestType != ArmonikRequestType.Execute)
    {
      return RequestTypeBalancer(dataSynapsePayload);
    }

    var methodName = dataSynapsePayload.MethodName;
    if (methodName == null)
    {
      throw new WorkerApiException($"Method name is empty in Service class [{GridAppNamespace}.{GridServiceName}]");
    }


    var arguments = dataSynapsePayload.SerializedArguments
                      ? new object?[]
                        {
                          dataSynapsePayload.ClientPayload,
                        }
                      : ProtoSerializer.DeSerializeMessageObjectArray(dataSynapsePayload.ClientPayload);

    var methodInfo = arguments != null
                       ? ServiceClass?.GetType()
                                     .GetMethod(methodName,
                                                arguments.Select(x => x!.GetType())
                                                         .ToArray())
                       : ServiceClass?.GetType()
                                     .GetMethod(methodName);
    if (methodInfo == null)
    {
      throw new
        WorkerApiException($"Cannot found method [{methodName}({string.Join(", ", arguments?.Select(x => x!.GetType().Name) ?? Array.Empty<string>())})] in Service class [{GridAppNamespace}.{GridServiceName}]");
    }

    try
    {
      var result = methodInfo.IsStatic
                     ? methodInfo.Invoke(null,
                                         arguments)
                     : methodInfo.Invoke(ServiceClass,
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
    catch (InvalidOperationException e)
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

    return new byte[]
           {
           };
  }

  public void SessionFinalize()
    => ServiceInvocationContext = null;

  public void DestroyService()
    => Dispose();

  /// <summary>Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.</summary>
  public void Dispose()
  {
    SessionFinalize();
    ServiceAdminWorker?.Dispose();
  }

  private byte[] RequestTypeBalancer(ArmonikPayload? dataSynapsePayload)
  {
    switch (dataSynapsePayload?.ArmonikRequestType)
    {
      case ArmonikRequestType.Upload:
        return ServiceAdminWorker?.UploadResources("TODO") ?? Array.Empty<byte>();
      default:
        return Array.Empty<byte>();
    }
  }
}
