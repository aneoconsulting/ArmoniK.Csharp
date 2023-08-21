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

using ArmoniK.DevelopmentKit.Client.Common.Submitter.ApiExt;

using JetBrains.Annotations;

namespace ArmoniK.DevelopmentKit.Client.Common.Status;

/// <summary>
///   Stores the relation between result id, task id and result status
/// </summary>
/// <param name="ResultId">The id of the result</param>
/// <param name="TaskId">The id of the task producing the result</param>
/// <param name="Status">The status of the result</param>
[PublicAPI]
public sealed record ResultStatusData(string              ResultId,
                                      string              TaskId,
                                      ArmoniKResultStatus Status);
