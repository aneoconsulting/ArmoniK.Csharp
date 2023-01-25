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
using System.Data;
using System.IO;
using System.Reflection;
using System.Runtime.Loader;

using ArmoniK.DevelopmentKit.Common;
using ArmoniK.DevelopmentKit.Common.Exceptions;
using ArmoniK.DevelopmentKit.Worker.Common;
using ArmoniK.DevelopmentKit.Worker.Common.Archive;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace ArmoniK.DevelopmentKit.Worker.DLLWorker;

public class AppsLoader : IAppsLoader
{
  private readonly Assembly   assemblyGridWorker_;
  private readonly EngineType engineType_;

  private readonly ILogger<AppsLoader>? logger_;
  private          Assembly             assembly_;

  public AppsLoader(IConfiguration? configuration,
                    ILoggerFactory? loggerFactory,
                    string          engineTypeAssemblyName,
                    IFileAdapter    fileAdapter,
                    string          fileName)
  {
    Configuration = configuration;

    engineType_ = EngineTypeHelper.ToEnum(engineTypeAssemblyName);

    FileAdapter = fileAdapter;

    ArmoniKDevelopmentKitServerApi = new EngineTypes()[engineType_];

    logger_ = loggerFactory?.CreateLogger<AppsLoader>();

    var archiver = new ZipArchiver();

    if (!archiver.ArchiveAlreadyExtracted(fileAdapter,
                                          fileName))
    {
      archiver.DownloadArchive(fileAdapter,
                               fileName);
      archiver.ExtractArchive(fileAdapter,
                              fileName);
    }


    var localPathToAssembly = ZipArchiver.GetLocalPathToAssembly(Path.Combine(fileAdapter.DestinationDirPath,
                                                                              fileName));

    UserAssemblyLoadContext = new AddonsAssemblyLoadContext(localPathToAssembly);

    try
    {
      assembly_ = UserAssemblyLoadContext.LoadFromAssemblyPath(localPathToAssembly);
    }
    catch (Exception ex)
    {
      logger_?.LogError($"Cannot load assembly from path [${localPathToAssembly}] "             + ex.Message + Environment.NewLine + ex.StackTrace);
      throw new WorkerApiException($"Cannot load assembly from path [${localPathToAssembly}] " + ex.Message + Environment.NewLine + ex.StackTrace);
    }

    PathToAssembly = localPathToAssembly;

    var localPathToAssemblyGridWorker = $"{Path.GetDirectoryName(localPathToAssembly)}/{ArmoniKDevelopmentKitServerApi}.dll";

    try
    {
      assemblyGridWorker_ = UserAssemblyLoadContext.LoadFromAssemblyPath(localPathToAssemblyGridWorker);
    }
    catch (Exception ex)
    {
      logger_?.LogError($"Cannot load assembly from path [${localPathToAssemblyGridWorker}] "             + ex.Message + Environment.NewLine + ex.StackTrace);
      throw new WorkerApiException($"Cannot load assembly from path [${localPathToAssemblyGridWorker}] " + ex.Message + Environment.NewLine + ex.StackTrace);
    }

    var location = Path.GetDirectoryName(localPathToAssembly);
    if (location != null)
    {
      Directory.SetCurrentDirectory(location);
      logger_?.LogInformation($"Set Default path to [${location}]");
    }

    logger_?.LogInformation($"GridWorker assembly from path [${localPathToAssemblyGridWorker}]");

    PathToAssemblyGridWorker = localPathToAssemblyGridWorker;

    var currentDomain = AppDomain.CurrentDomain;
    currentDomain.AssemblyResolve += LoadFromSameFolder;
  }

  Assembly? LoadFromSameFolder(object?          sender,
                               ResolveEventArgs args)
  {
    var folderPath = Path.GetDirectoryName(PathToAssembly);
    var assemblyPath = Path.Combine(folderPath ?? "",
                                    new AssemblyName(args.Name).Name + ".dll");

    Assembly assembly;

    try
    {
      assembly = Assembly.LoadFrom(assemblyPath);
    }
    catch (Exception)
    {
      folderPath = "/app";
      assemblyPath = Path.Combine(folderPath,
                                  new AssemblyName(args.Name).Name + ".dll");

      if (!File.Exists(assemblyPath))
      {
        return null;
      }

      assembly = Assembly.LoadFrom(assemblyPath);
    }

    return assembly;
  }

  private string ArmoniKDevelopmentKitServerApi { get; }

  public AssemblyLoadContext UserAssemblyLoadContext { get; private set; }

  public IConfiguration? Configuration { get; }

  public IFileAdapter FileAdapter { get; set; }

  public string PathToAssembly { get; set; }

  public string PathToAssemblyGridWorker { get; set; }

  public T GetServiceContainerInstance<T>(string? appNamespace,
                                          string? serviceContainerClassName)
  {
    using (UserAssemblyLoadContext.EnterContextualReflection())
    {
      // Create an instance of a class from the assembly.
      var classType = assembly_.GetType($"{appNamespace}.{serviceContainerClassName}");

      if (classType != null)
      {
        var serviceContainer = (T?)Activator.CreateInstance(classType) ?? throw new WorkerApiException($"Cannot find constructor for : {nameof(classType)}");

        return serviceContainer;
      }
    }

    throw new WorkerApiException($"Cannot find ServiceContainer named : {appNamespace}.{serviceContainerClassName} in dll [{PathToAssembly}]");
  }

  public void Dispose()
  {
    UserAssemblyLoadContext.Unload();
  }

  public IGridWorker GetGridWorkerInstance(IConfiguration? configuration,
                                           ILoggerFactory? loggerFactory)
  {
    // Create an instance of a class from the assembly.
    try
    {
      using (UserAssemblyLoadContext.EnterContextualReflection())
      {
        var classType = assemblyGridWorker_.GetType($"{ArmoniKDevelopmentKitServerApi}.GridWorker");

        if (classType != null)
        {
          var args = new object?[]
                     {
                       configuration,
                       loggerFactory,
                     };


          var gridWorker = (IGridWorker?)Activator.CreateInstance(classType,
                                                                  args);
          if (gridWorker != null)
          {
            return gridWorker;
          }
        }
      }
    }
    catch (Exception e)
    {
      Console.WriteLine(e);
      throw new WorkerApiException(e);
    }

    throw new NullReferenceException($"Cannot find ServiceContainer named : {ArmoniKDevelopmentKitServerApi}.GridWorker in dll [{PathToAssemblyGridWorker}]");
  }

  public T GetServiceContainerInstance<T>(string          appNamespace,
                                          string          serviceContainerClassName,
                                          params object[] args)
  {
    using (UserAssemblyLoadContext.EnterContextualReflection())
    {
      // Create an instance of a class from the assembly.
      var classType = assembly_.GetType($"{appNamespace}.{serviceContainerClassName}");

      if (classType != null)
      {
        var serviceContainer = (T?)Activator.CreateInstance(classType,
                                                            args);
        if (serviceContainer != null)
        {
          return serviceContainer;
        }
      }
    }

    Dispose();
    throw new NullReferenceException($"Cannot find ServiceContainer named : {appNamespace}.{serviceContainerClassName} in dll [{PathToAssembly}]");
  }

  ~AppsLoader()
    => Dispose();

  public bool RequestNewAssembly(string engineType,
                                 string pathToZipFile)
  {
    if (pathToZipFile == null)
    {
      throw new ArgumentNullException(nameof(pathToZipFile),
                                      "pathToZipFile is a null argument");
    }

    return engineType == null || engineType_ != EngineTypeHelper.ToEnum(engineType) || FileAdapter == null || !pathToZipFile.Equals(FileAdapter);
  }
}
