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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.DevelopmentKit.Common;
using ArmoniK.DevelopmentKit.Common.Exceptions;
using ArmoniK.DevelopmentKit.Worker.Common;
using ArmoniK.DevelopmentKit.Worker.Common.Adaptater;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace ArmoniK.DevelopmentKit.Worker.DLLWorker;

public class ServiceId
{
  public ServiceId(string engineTypeName,
                   string pathToZipFile,
                   string namespaceService)
    => Key = $"{engineTypeName}#{pathToZipFile}#{namespaceService}".ToLower();

  public string Key { get; set; }

  /// <summary>Returns a string that represents the current object.</summary>
  /// <returns>A string that represents the current object.</returns>
  public override string ToString()
    => Key;
}

public class ArmonikServiceWorker : IDisposable
{
  public ArmonikServiceWorker()
    => Initialized = false;

  public AppsLoader  AppsLoader { get; set; }
  public IGridWorker GridWorker { get; set; }

  public bool Initialized { get; set; }

  /// <summary>Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.</summary>
  public void Dispose()
  {
    using (AppsLoader.UserAssemblyLoadContext.EnterContextualReflection())
    {
      GridWorker?.Dispose();
    }

    GridWorker = null;
    AppsLoader.Dispose();
    AppsLoader = null;
  }

  public void CloseSession()
  {
    using (AppsLoader.UserAssemblyLoadContext.EnterContextualReflection())
    {
      GridWorker?.SessionFinalize();
    }
  }

  public void Configure(IConfiguration configuration,
                        TaskOptions    requestTaskOptions)
  {
    if (Initialized)
    {
      return;
    }

    using (AppsLoader.UserAssemblyLoadContext.EnterContextualReflection())
    {
      GridWorker.Configure(configuration,
                           requestTaskOptions,
                           AppsLoader);
    }

    Initialized = true;
  }

  public void InitializeSessionWorker(Session     sessionId,
                                      TaskOptions taskHandlerTaskOptions)
  {
    using (AppsLoader.UserAssemblyLoadContext.EnterContextualReflection())
    {
      GridWorker.InitializeSessionWorker(sessionId,
                                         taskHandlerTaskOptions);
    }
  }

  public byte[] Execute(ITaskHandler taskHandler)
  {
    using (AppsLoader.UserAssemblyLoadContext.EnterContextualReflection())
    {
      return GridWorker.Execute(taskHandler);
    }
  }
}

public class ServiceRequestContext
{
  public ServiceRequestContext(ILoggerFactory loggerFactory)
  {
    LoggerFactory  = loggerFactory;
    ServicesMapper = new Dictionary<string, ArmonikServiceWorker>();
  }

  public Session SessionId { get; set; }

  public  ILoggerFactory                            LoggerFactory  { get; set; }
  private IDictionary<string, ArmonikServiceWorker> ServicesMapper { get; }

  public bool IsNewSessionId(Session sessionId)
  {
    if (SessionId == null)
    {
      return true;
    }

    return SessionId.Id != sessionId.Id;
  }

  public bool IsNewSessionId(string sessionId)
  {
    if (sessionId == null)
    {
      throw new ArgumentNullException(nameof(sessionId));
    }

    if (SessionId == null)
    {
      return true;
    }

    var currentSessionId = new Session
                           {
                             Id = sessionId,
                           };

    return IsNewSessionId(currentSessionId);
  }

  public ArmonikServiceWorker CreateOrGetArmonikService(IConfiguration configuration,
                                                        string         engineTypeName,
                                                        IFileAdaptater fileAdaptater,
                                                        string         fileName,
                                                        TaskOptions    requestTaskOptions)
  {
    if (string.IsNullOrEmpty(requestTaskOptions.ApplicationNamespace))
    {
      throw new WorkerApiException("Cannot find namespace service in TaskOptions. Please set the namespace");
    }

    ArmonikServiceWorker armonikServiceWorker;

    var serviceId = GenerateServiceId(engineTypeName,
                                      Path.Combine(fileAdaptater.DestinationDirPath,
                                                   fileName),
                                      requestTaskOptions.ApplicationNamespace);

    if (ServicesMapper.ContainsKey(serviceId.Key))
    {
      armonikServiceWorker = ServicesMapper[serviceId.Key];

      armonikServiceWorker.Configure(configuration,
                                     requestTaskOptions);

      return armonikServiceWorker;
    }

    var appsLoader = new AppsLoader(configuration,
                                    LoggerFactory,
                                    engineTypeName,
                                    fileAdaptater,
                                    fileName);

    armonikServiceWorker = new ArmonikServiceWorker
                           {
                             AppsLoader = appsLoader,
                             GridWorker = appsLoader.GetGridWorkerInstance(configuration,
                                                                           LoggerFactory),
                           };

    ServicesMapper[serviceId.Key] = armonikServiceWorker;

    armonikServiceWorker.Configure(configuration,
                                   requestTaskOptions);
    return armonikServiceWorker;
  }

  public static ServiceId GenerateServiceId(string engineTypeName,
                                            string uniqueKey,
                                            string namespaceService)
    => new(engineTypeName,
           uniqueKey,
           namespaceService);

  public static IFileAdaptater CreateOrGetFileAdaptater(IConfiguration configuration,
                                                        string         localDirectoryZip)
  {
    var sectionStorage = configuration.GetSection("FileStorageType");
    if (sectionStorage.Exists() && configuration["FileStorageType"] == "FS")
    {
      return new FsAdaptater(localDirectoryZip);
    }

    if ((sectionStorage.Exists() && configuration["FileStorageType"] == "S3") || !sectionStorage.Exists())
    {
      return new S3Adaptater(configuration.GetSection("S3Storage")["ServiceURL"],
                             configuration.GetSection("S3Storage")["BucketName"],
                             configuration.GetSection("S3Storage")["AccessKeyId"],
                             configuration.GetSection("S3Storage")["SecretAccessKey"],
                             "");
    }

    throw new WorkerApiException("Cannot find the FileStorageType in the IConfiguration. Please make sure you have properly set the field [FileStorageType]");
  }
}
