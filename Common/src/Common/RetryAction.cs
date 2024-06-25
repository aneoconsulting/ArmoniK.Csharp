// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

namespace ArmoniK.DevelopmentKit.Common;

/// <summary>
///   A generic class to implement retry function
/// </summary>
public static class Retry
{
  /// <summary>
  ///   Retry the specified action at most retries times until it returns true.
  /// </summary>
  /// <param name="retries">The number of times to retry the operation</param>
  /// <param name="delayMs">The number of milliseconds to sleep after a failed invocation of the operation</param>
  /// <param name="operation">The operation to perform. Should return true</param>
  /// <returns>true if the action returned true on one of the retries, false if the number of retries was exhausted</returns>
  public static bool UntilTrue(int        retries,
                               int        delayMs,
                               Func<bool> operation)
  {
    for (var retry = 0; retry < retries; retry++)
    {
      if (operation())
      {
        return true;
      }

      Thread.Sleep(delayMs);
    }

    return false;
  }

  /// <summary>
  ///   Retry the specified operation the specified number of times, until there are no more retries or it succeeded
  ///   without an exception.
  /// </summary>
  /// <param name="retries">The number of times to retry the operation</param>
  /// <param name="delayMs">The number of milliseconds to sleep after a failed invocation of the operation</param>
  /// <param name="operation">the operation to perform</param>
  /// <param name="exceptionType">if not null, ignore any exceptions of this type and subtypes</param>
  /// <param name="allowDerivedExceptions">
  ///   If true, exceptions deriving from the specified exception type are ignored as
  ///   well. Defaults to False
  /// </param>
  /// <returns>When one of the retries succeeds, return the value the operation returned. If not, an exception is thrown.</returns>
  public static void WhileException(int           retries,
                                    int           delayMs,
                                    Action<int>   operation,
                                    bool          allowDerivedExceptions = false,
                                    params Type[] exceptionType)
    => WhileException(retries,
                      delayMs,
                      operation,
                      allowDerivedExceptions,
                      null,
                      exceptionType);

  /// <summary>
  ///   Retry the specified operation the specified number of times, until there are no more retries or it succeeded
  ///   without an exception.
  /// </summary>
  /// <param name="retries">The number of times to retry the operation</param>
  /// <param name="delayMs">The number of milliseconds to sleep after a failed invocation of the operation</param>
  /// <param name="operation">the operation to perform</param>
  /// <param name="exceptionType">if not null, ignore any exceptions of this type and subtypes</param>
  /// <param name="allowDerivedExceptions">
  ///   If true, exceptions deriving from the specified exception type are ignored as
  ///   well. Defaults to False
  /// </param>
  /// <param name="logger">Logger to log retried exception</param>
  /// <returns>When one of the retries succeeds, return the value the operation returned. If not, an exception is thrown.</returns>
  public static void WhileException(int           retries,
                                    int           delayMs,
                                    Action<int>   operation,
                                    bool          allowDerivedExceptions = false,
                                    ILogger?      logger                 = null,
                                    params Type[] exceptionType)
  {
    // Do all but one retries in the loop
    for (var retry = 1; retry < retries; retry++)
    {
      try
      {
        // Try the operation. If it succeeds, return its result
        operation(retry);
        return;
      }
      catch (Exception ex)
      {
        // Oops - it did NOT succeed!
        if (exceptionType != null && allowDerivedExceptions && ex is AggregateException &&
            exceptionType.Any(e => ex.InnerException != null && ex.InnerException.GetType() == e))
        {
          logger?.LogWarning(ex,
                             "Got exception while executing function to retry {retry}/{retries}",
                             retry,
                             retries);
          Thread.Sleep(delayMs);
        }
        else if (exceptionType == null || exceptionType.Any(e => e == ex.GetType()) || (allowDerivedExceptions && exceptionType.Any(e => ex.GetType()
                                                                                                                                           .IsSubclassOf(e))))
        {
          // Ignore exceptions when exceptionType is not specified OR
          // the exception thrown was of the specified exception type OR
          // the exception thrown is derived from the specified exception type and we allow that
          logger?.LogWarning(ex,
                             "Got exception while executing function to retry {retry}/{retries}",
                             retry,
                             retries);
          Thread.Sleep(delayMs);
        }
        else
        {
          // We have an unexpected exception! Re-throw it:
          throw;
        }
      }
    }
  }

  /// <summary>
  ///   Retry the specified operation the specified number of times, until there are no more retries or it succeeded
  ///   without an exception.
  /// </summary>
  /// <typeparam name="T">The return type of the exception</typeparam>
  /// <param name="retries">The number of times to retry the operation</param>
  /// <param name="delayMs">The number of milliseconds to sleep after a failed invocation of the operation</param>
  /// <param name="operation">the operation to perform</param>
  /// <param name="exceptionType">if not null, ignore any exceptions of this type and subtypes</param>
  /// <param name="allowDerivedExceptions">
  ///   If true, exceptions deriving from the specified exception type are ignored as
  ///   well. Defaults to False
  /// </param>
  /// <returns>When one of the retries succeeds, return the value the operation returned. If not, an exception is thrown.</returns>
  public static T WhileException<T>(int           retries,
                                    int           delayMs,
                                    Func<int, T>  operation,
                                    bool          allowDerivedExceptions = false,
                                    params Type[] exceptionType)
    => WhileException(retries,
                      delayMs,
                      operation,
                      allowDerivedExceptions,
                      null,
                      exceptionType);

  /// <summary>
  ///   Retry the specified operation the specified number of times, until there are no more retries or it succeeded
  ///   without an exception.
  /// </summary>
  /// <typeparam name="T">The return type of the exception</typeparam>
  /// <param name="retries">The number of times to retry the operation</param>
  /// <param name="delayMs">The number of milliseconds to sleep after a failed invocation of the operation</param>
  /// <param name="operation">the operation to perform</param>
  /// <param name="exceptionType">if not null, ignore any exceptions of this type and subtypes</param>
  /// <param name="allowDerivedExceptions">
  ///   If true, exceptions deriving from the specified exception type are ignored as
  ///   well. Defaults to False
  /// </param>
  /// <param name="logger">Logger to log retried exception</param>
  /// <returns>When one of the retries succeeds, return the value the operation returned. If not, an exception is thrown.</returns>
  public static T WhileException<T>(int           retries,
                                    int           delayMs,
                                    Func<int, T>  operation,
                                    bool          allowDerivedExceptions = false,
                                    ILogger?      logger                 = null,
                                    params Type[] exceptionType)
  {
    // Do all but one retries in the loop
    for (var retry = 1; retry < retries; retry++)
    {
      try
      {
        // Try the operation. If it succeeds, return its result
        return operation(retry);
      }
      catch (Exception ex)
      {
        if (exceptionType != null && allowDerivedExceptions && ex is AggregateException &&
            exceptionType.Any(e => ex.InnerException != null && ex.InnerException.GetType() == e))
        {
          logger?.LogWarning(ex,
                             "Got exception while executing function to retry {retry}/{retries}",
                             retry,
                             retries);
          Thread.Sleep(delayMs);
        }
        else if (exceptionType == null || exceptionType.Any(e => e == ex.GetType()) || (allowDerivedExceptions && exceptionType.Any(e => ex.GetType()
                                                                                                                                           .IsSubclassOf(e))))
        {
          // Ignore exceptions when exceptionType is not specified OR
          // the exception thrown was of the specified exception type OR
          // the exception thrown is derived from the specified exception type and we allow that
          logger?.LogWarning(ex,
                             "Got exception while executing function to retry {retry}/{retries}",
                             retry,
                             retries);
          Thread.Sleep(delayMs);
        }
        else
        {
          // We have an unexpected exception! Re-throw it:
          throw;
        }
      }
    }

    // Try the operation one last time. This may or may not succeed.
    // Exceptions pass unchanged. If this is an expected exception we need to know about it because
    // we're out of retries. If it's unexpected, throwing is the right thing to do anyway
    return operation(retries);
  }


  /// <summary>
  ///   Retry the specified operation the specified number of times, until there are no more retries or it succeeded
  ///   without an exception.
  /// </summary>
  /// <param name="retries">The number of times to retry the operation</param>
  /// <param name="delayMs">The number of milliseconds to sleep after a failed invocation of the operation</param>
  /// <param name="operation">the operation to perform</param>
  /// <param name="cancellationToken"></param>
  /// <param name="exceptionType">if not null, ignore any exceptions of this type and subtypes</param>
  /// <param name="allowDerivedExceptions">
  ///   If true, exceptions deriving from the specified exception type are ignored as
  ///   well. Defaults to False
  /// </param>
  /// <returns>When one of the retries succeeds, return the value the operation returned. If not, an exception is thrown.</returns>
  public static ValueTask WhileException(int                  retries,
                                         int                  delayMs,
                                         Func<int, ValueTask> operation,
                                         bool                 allowDerivedExceptions = false,
                                         CancellationToken    cancellationToken      = default,
                                         params Type[]        exceptionType)
    => WhileException(retries,
                      delayMs,
                      operation,
                      allowDerivedExceptions,
                      null,
                      cancellationToken,
                      exceptionType);

  /// <summary>
  ///   Retry the specified operation the specified number of times, until there are no more retries or it succeeded
  ///   without an exception.
  /// </summary>
  /// <param name="retries">The number of times to retry the operation</param>
  /// <param name="delayMs">The number of milliseconds to sleep after a failed invocation of the operation</param>
  /// <param name="operation">the operation to perform</param>
  /// <param name="cancellationToken"></param>
  /// <param name="exceptionType">if not null, ignore any exceptions of this type and subtypes</param>
  /// <param name="allowDerivedExceptions">
  ///   If true, exceptions deriving from the specified exception type are ignored as
  ///   well. Defaults to False
  /// </param>
  /// <param name="logger">Logger to log retried exception</param>
  /// <returns>When one of the retries succeeds, return the value the operation returned. If not, an exception is thrown.</returns>
  public static async ValueTask WhileException(int                  retries,
                                               int                  delayMs,
                                               Func<int, ValueTask> operation,
                                               bool                 allowDerivedExceptions = false,
                                               ILogger?             logger                 = null,
                                               CancellationToken    cancellationToken      = default,
                                               params Type[]        exceptionType)
  {
    // Do all but one retries in the loop
    for (var retry = 1; retry < retries; retry++)
    {
      try
      {
        // Try the operation. If it succeeds, return its result
        await operation(retry)
          .ConfigureAwait(false);
        return;
      }
      catch (Exception ex)
      {
        // Oops - it did NOT succeed!
        if (exceptionType != null && allowDerivedExceptions && ex is AggregateException &&
            exceptionType.Any(e => ex.InnerException != null && ex.InnerException.GetType() == e))
        {
          logger?.LogWarning(ex,
                             "Got exception while executing function to retry {retry}/{retries}",
                             retry,
                             retries);
          Thread.Sleep(delayMs);
        }
        else if (exceptionType == null || exceptionType.Any(e => e == ex.GetType()) || (allowDerivedExceptions && exceptionType.Any(e => ex.GetType()
                                                                                                                                           .IsSubclassOf(e))))
        {
          // Ignore exceptions when exceptionType is not specified OR
          // the exception thrown was of the specified exception type OR
          // the exception thrown is derived from the specified exception type and we allow that
          logger?.LogWarning(ex,
                             "Got exception while executing function to retry {retry}/{retries}",
                             retry,
                             retries);
          await Task.Delay(delayMs,
                           cancellationToken)
                    .ConfigureAwait(false);
        }
        else
        {
          // We have an unexpected exception! Re-throw it:
          throw;
        }
      }
    }
  }

  /// <summary>
  ///   Retry the specified operation the specified number of times, until there are no more retries or it succeeded
  ///   without an exception.
  /// </summary>
  /// <typeparam name="T">The return type of the exception</typeparam>
  /// <param name="retries">The number of times to retry the operation</param>
  /// <param name="delayMs">The number of milliseconds to sleep after a failed invocation of the operation</param>
  /// <param name="operation">the operation to perform</param>
  /// <param name="cancellationToken"></param>
  /// <param name="exceptionType">if not null, ignore any exceptions of this type and subtypes</param>
  /// <param name="allowDerivedExceptions">
  ///   If true, exceptions deriving from the specified exception type are ignored as
  ///   well. Defaults to False
  /// </param>
  /// <returns>When one of the retries succeeds, return the value the operation returned. If not, an exception is thrown.</returns>
  public static ValueTask<T> WhileException<T>(int                     retries,
                                               int                     delayMs,
                                               Func<int, ValueTask<T>> operation,
                                               bool                    allowDerivedExceptions = false,
                                               CancellationToken       cancellationToken      = default,
                                               params Type[]           exceptionType)
    => WhileException(retries,
                      delayMs,
                      operation,
                      allowDerivedExceptions,
                      null,
                      cancellationToken,
                      exceptionType);

  /// <summary>
  ///   Retry the specified operation the specified number of times, until there are no more retries or it succeeded
  ///   without an exception.
  /// </summary>
  /// <typeparam name="T">The return type of the exception</typeparam>
  /// <param name="retries">The number of times to retry the operation</param>
  /// <param name="delayMs">The number of milliseconds to sleep after a failed invocation of the operation</param>
  /// <param name="operation">the operation to perform</param>
  /// <param name="cancellationToken"></param>
  /// <param name="exceptionType">if not null, ignore any exceptions of this type and subtypes</param>
  /// <param name="allowDerivedExceptions">
  ///   If true, exceptions deriving from the specified exception type are ignored as
  ///   well. Defaults to False
  /// </param>
  /// <param name="logger">Logger to log retried exception</param>
  /// <returns>When one of the retries succeeds, return the value the operation returned. If not, an exception is thrown.</returns>
  public static async ValueTask<T> WhileException<T>(int                     retries,
                                                     int                     delayMs,
                                                     Func<int, ValueTask<T>> operation,
                                                     bool                    allowDerivedExceptions = false,
                                                     ILogger?                logger                 = null,
                                                     CancellationToken       cancellationToken      = default,
                                                     params Type[]           exceptionType)
  {
    // Do all but one retries in the loop
    for (var retry = 1; retry < retries; retry++)
    {
      try
      {
        // Try the operation. If it succeeds, return its result
        return await operation(retry)
                 .ConfigureAwait(false);
      }
      catch (Exception ex)
      {
        if (exceptionType != null && allowDerivedExceptions && ex is AggregateException &&
            exceptionType.Any(e => ex.InnerException != null && ex.InnerException.GetType() == e))
        {
          logger?.LogWarning(ex,
                             "Got exception while executing function to retry {retry}/{retries}",
                             retry,
                             retries);
          await Task.Delay(delayMs,
                           cancellationToken)
                    .ConfigureAwait(false);
        }
        else if (exceptionType == null || exceptionType.Any(e => e == ex.GetType()) || (allowDerivedExceptions && exceptionType.Any(e => ex.GetType()
                                                                                                                                           .IsSubclassOf(e))))
        {
          // Ignore exceptions when exceptionType is not specified OR
          // the exception thrown was of the specified exception type OR
          // the exception thrown is derived from the specified exception type and we allow that
          logger?.LogWarning(ex,
                             "Got exception while executing function to retry {retry}/{retries}",
                             retry,
                             retries);
          await Task.Delay(delayMs,
                           cancellationToken)
                    .ConfigureAwait(false);
        }
        else
        {
          // We have an unexpected exception! Re-throw it:
          throw;
        }
      }
    }

    // Try the operation one last time. This may or may not succeed.
    // Exceptions pass unchanged. If this is an expected exception we need to know about it because
    // we're out of retries. If it's unexpected, throwing is the right thing to do anyway
    return await operation(retries)
             .ConfigureAwait(false);
  }
}
