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

using ArmoniK.Api.gRPC.V1;

namespace ArmoniK.DevelopmentKit.Client.Common.Status;

/// <summary>
///   Extends the ArmoniK API TaskStatus type to provide some means for conversion.
/// </summary>
public static class TaskStatusExt
{
  /// <summary>
  ///   Converts the status from native API representation to SDK representation
  /// </summary>
  /// <param name="taskStatus">the native API status to convert</param>
  /// <returns>the SDK status</returns>
  public static ArmonikTaskStatusCode ToArmonikStatusCode(this TaskStatus taskStatus)
    => taskStatus switch
       {
         TaskStatus.Submitted   => ArmonikTaskStatusCode.ResultNotReady,
         TaskStatus.Timeout     => ArmonikTaskStatusCode.TaskTimeout,
         TaskStatus.Cancelled   => ArmonikTaskStatusCode.TaskCancelled,
         TaskStatus.Cancelling  => ArmonikTaskStatusCode.TaskCancelled,
         TaskStatus.Error       => ArmonikTaskStatusCode.TaskFailed,
         TaskStatus.Processing  => ArmonikTaskStatusCode.ResultNotReady,
         TaskStatus.Dispatched  => ArmonikTaskStatusCode.ResultNotReady,
         TaskStatus.Completed   => ArmonikTaskStatusCode.TaskCompleted,
         TaskStatus.Creating    => ArmonikTaskStatusCode.ResultNotReady,
         TaskStatus.Unspecified => ArmonikTaskStatusCode.Unknown,
         TaskStatus.Processed   => ArmonikTaskStatusCode.ResultReady,
         _                      => ArmonikTaskStatusCode.Unknown,
       };
}
