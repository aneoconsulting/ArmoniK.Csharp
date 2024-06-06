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
using System.IO;
using System.IO.Compression;
using System.Threading;

using ArmoniK.DevelopmentKit.Common.Exceptions;
using ArmoniK.DevelopmentKit.Common.Utils;

namespace ArmoniK.DevelopmentKit.Worker.Common.Archive;

/// <summary>
///   Class used do handle zip archives
/// </summary>
public class ZipArchiver : IArchiver
{
  private readonly string rootAppPath_;

  /// <summary>
  ///   Creates a zip archive handler
  /// </summary>
  /// <param name="assembliesBasePath">Base path to extract zip files</param>
  public ZipArchiver(string assembliesBasePath)
    => rootAppPath_ = assembliesBasePath;

  /// <inheritdoc />
  public bool ArchiveAlreadyExtracted(PackageId packageId,
                                      int       waitForExtraction = 60000,
                                      int       spinInterval      = 1000)
  {
    var pathToAssemblyDir = Path.Combine(rootAppPath_,
                                         packageId.PackageSubpath);

    if (!Directory.Exists(pathToAssemblyDir))
    {
      return false;
    }

    var mainAssembly = Path.Combine(pathToAssemblyDir,
                                    packageId.MainAssemblyFileName);

    if (File.Exists(mainAssembly))
    {
      return true;
    }

    var lockFileName = Path.Combine(pathToAssemblyDir,
                                    $"{packageId.ApplicationName}.lock");

    while (File.Exists(lockFileName) && waitForExtraction > 0)
    {
      Thread.Sleep(Math.Min(spinInterval,
                            waitForExtraction));
      waitForExtraction -= spinInterval;
    }

    return File.Exists(mainAssembly) && !File.Exists(lockFileName);
  }

  /// <inheritdoc cref="IArchiver" />
  /// <exception cref="WorkerApiException">
  ///   Thrown if the file isn't a zip archive or if the lock file isn't lockable when the
  ///   archive is being extracted by another process
  /// </exception>
  public string ExtractArchive(string    filename,
                               PackageId packageId,
                               bool      overwrite = false)
  {
    if (!IsZipFile(filename))
    {
      throw new WorkerApiException("Cannot yet extract or manage raw data other than zip archive");
    }

    var pathToAssemblyDir = Path.Combine(rootAppPath_,
                                         packageId.PackageSubpath);
    var mainAssembly = Path.Combine(pathToAssemblyDir,
                                    packageId.MainAssemblyFileName);

    if (!Directory.Exists(pathToAssemblyDir))
    {
      Directory.CreateDirectory(pathToAssemblyDir);
    }

    var lockFileName = Path.Combine(pathToAssemblyDir,
                                    $"{packageId.ApplicationName}.lock");

    using (var spinLock = new FileSpinLock(lockFileName,
                                           timeoutMs: 60000))
    {
      if (spinLock.HasLock)
      {
        if (overwrite || !File.Exists(mainAssembly))
        {
          try
          {
            ZipFile.ExtractToDirectory(filename,
                                       rootAppPath_);
          }
          catch (Exception e)
          {
            throw new WorkerApiException($"Could not extract zip file {filename}",
                                         e);
          }
        }
      }
      else
      {
        throw new WorkerApiException($"Could not lock file to extract zip {filename}");
      }
    }

    //Check now if the assembly is present
    if (!File.Exists(mainAssembly))
    {
      var errorMessage = Directory.Exists(Path.Combine(rootAppPath_,
                                                       packageId.ApplicationName))
                           ? Directory.Exists(pathToAssemblyDir)
                               ? $"{packageId.MainAssemblyFileName} as second folder"
                               : $"{filename} as fileName"
                           : $"{packageId.ApplicationName} as first folder";

      throw new WorkerApiException($"Fail to find assembly {mainAssembly}. The extracted zip should have the following structure"         +
                                   $" {packageId.ApplicationName}/{packageId.ApplicationVersion}/ which will contains all the dll files." +
                                   $"Expected: {errorMessage} ");
    }

    return pathToAssemblyDir;
  }

  /// <summary>
  /// </summary>
  /// <param name="assemblyNameFilePath"></param>
  /// <returns></returns>
  public static bool IsZipFile(string assemblyNameFilePath)
  {
    //ATm ONLY Check the extensions 

    var extension = Path.GetExtension(assemblyNameFilePath);
    return extension?.ToLower() == ".zip";
  }
}
