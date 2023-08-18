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

using ArmoniK.DevelopmentKit.Client.Common;
using ArmoniK.DevelopmentKit.Client.Unified.Factory;
using ArmoniK.DevelopmentKit.Client.Unified.Services.Common;

using Microsoft.Extensions.Logging;

namespace ArmoniK.DevelopmentKit.Client.Unified.Services.Admin;

/// <summary>
///   The class to access to all Admin and monitoring API
/// </summary>
public class ServiceAdmin : AbstractClientService
{
  /// <summary>
  ///   The constructor of the service Admin class
  /// </summary>
  /// <param name="properties">the properties setting to connection to the control plane</param>
  /// <param name="loggerFactory"></param>
  public ServiceAdmin(Properties     properties,
                      ILoggerFactory loggerFactory)
    : base(loggerFactory)
  {
    SessionServiceFactory = new SessionServiceFactory(LoggerFactory);

    AdminMonitoringService = SessionServiceFactory.GetAdminMonitoringService(properties);
  }

  /// <summary>
  ///   the Properties that access to the control plane
  /// </summary>
  public AdminMonitoringService AdminMonitoringService { get; set; }

  private SessionServiceFactory SessionServiceFactory { get; }

  /// <summary>Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.</summary>
  public override void Dispose()
  {
  }
}
