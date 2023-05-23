using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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
          Thread.Sleep(delayMs);
        }
        else if (exceptionType == null || exceptionType.Any(e => e == ex.GetType()) || (allowDerivedExceptions && exceptionType.Any(e => ex.GetType()
                                                                                                                                           .IsSubclassOf(e))))
        {
          // Ignore exceptions when exceptionType is not specified OR
          // the exception thrown was of the specified exception type OR
          // the exception thrown is derived from the specified exception type and we allow that
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
          Thread.Sleep(delayMs);
        }
        else if (exceptionType == null || exceptionType.Any(e => e == ex.GetType()) || (allowDerivedExceptions && exceptionType.Any(e => ex.GetType()
                                                                                                                                           .IsSubclassOf(e))))
        {
          // Ignore exceptions when exceptionType is not specified OR
          // the exception thrown was of the specified exception type OR
          // the exception thrown is derived from the specified exception type and we allow that
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
  ///   Retry async the specified operation the specified number of times, until there are no more retries or it succeeded
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
  public static async Task WhileExceptionAsync(int           retries,
                                               int           delayMs,
                                               Action<int>   operation,
                                               bool          allowDerivedExceptions = false,
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
          Thread.Sleep(delayMs);
        }
        else if (exceptionType == null || exceptionType.Any(e => e == ex.GetType()) || (allowDerivedExceptions && exceptionType.Any(e => ex.GetType()
                                                                                                                                           .IsSubclassOf(e))))
        {
          // Ignore exceptions when exceptionType is not specified OR
          // the exception thrown was of the specified exception type OR
          // the exception thrown is derived from the specified exception type and we allow that
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
  ///   Retry async the specified operation the specified number of times, until there are no more retries or it succeeded
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
  public static async Task<T> WhileExceptionAsync<T>(int                retries,
                                                     int                delayMs,
                                                     Func<int, Task<T>> operation,
                                                     bool               allowDerivedExceptions = false,
                                                     params Type[]      exceptionType)
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
          Thread.Sleep(delayMs);
        }
        else if (exceptionType == null || exceptionType.Any(e => e == ex.GetType()) || (allowDerivedExceptions && exceptionType.Any(e => ex.GetType()
                                                                                                                                           .IsSubclassOf(e))))
        {
          // Ignore exceptions when exceptionType is not specified OR
          // the exception thrown was of the specified exception type OR
          // the exception thrown is derived from the specified exception type and we allow that
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
    return await operation(retries)
             .ConfigureAwait(false);
  }
}
