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

using ArmoniK.DevelopmentKit.Client.Common.Status;
using ArmoniK.DevelopmentKit.Common;

using JetBrains.Annotations;

namespace ArmoniK.DevelopmentKit.Client.Common.Exceptions;

/// <summary>
///   The service invocation exception. This class wil contain all error information of task or result
/// </summary>
[MarkDownDoc]
[PublicAPI]
public class ServiceInvocationException : Exception
{
  private readonly string message_ = "ServiceInvocationException during call function";

  /// <summary>
  ///   The default constructor
  /// </summary>
  /// <param name="message">The message to set for the exception</param>
  /// <param name="statusCode">the statusCode in the output</param>
  public ServiceInvocationException(string            message,
                                    ArmonikStatusCode statusCode)
  {
    message_   = message;
    StatusCode = statusCode;
  }

  /// <summary>
  ///   The default constructor
  /// </summary>
  /// <param name="e">The previous exception</param>
  public ServiceInvocationException(Exception e)
    : base(e.Message,
           e)
    => message_ = $"{message_} with InnerException {e.GetType()} message : {e.Message}";

  /// <summary>
  ///   The overriden constructor to accept inner Exception as parameters
  /// </summary>
  /// <param name="e">The previous exception</param>
  /// <param name="statusCode">The status of the task which is failing</param>
  public ServiceInvocationException(Exception         e,
                                    ArmonikStatusCode statusCode)
    : base(e.Message,
           e)
  {
    StatusCode = statusCode;
    message_   = $"{message_} with InnerException {e.GetType()} message : {e.Message}";
  }

  /// <summary>
  ///   The overriden constructor to acceptation inner exception and message as parameters
  /// </summary>
  /// <param name="message">The message to set in the exception</param>
  /// <param name="e">The previous exception generated by failure</param>
  /// <param name="statusCode">The status of the task which is failing</param>
  public ServiceInvocationException(string            message,
                                    ArgumentException e,
                                    ArmonikStatusCode statusCode)
    : base(message,
           e)
  {
    message_   = message;
    StatusCode = statusCode;
  }

  /// <summary>
  ///   The status code when error occurred
  /// </summary>
  public ArmonikStatusCode StatusCode { get; }

  /// <summary>
  ///   The error details coming from TaskOutput API
  /// </summary>
  public string OutputDetails { get; set; } = "";

  /// <summary>
  ///   Overriding the Message property
  /// </summary>
  public override string Message
    => message_;
}
