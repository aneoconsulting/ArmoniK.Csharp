// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ArmoniK.DevelopmentKit.Client.Common.Submitter;

/// <summary>
///   The class to map submitId to taskId when submit is done asynchronously
/// </summary>
public class RequestTaskMap
{
  private const    int                                         WaitTime    = 100;
  private readonly ConcurrentDictionary<Guid, RequestMapValue> dictionary_ = new();

  /// <summary>
  ///   Push the SubmitId and taskId in the concurrentDictionary
  /// </summary>
  /// <param name="SubmitId">The submit Id push during the submission</param>
  /// <param name="taskId">the taskId was given by the control Plane</param>
  public void PutResponse(Guid   SubmitId,
                          string taskId)
    => dictionary_[SubmitId] = new RequestMapValue(taskId);

  /// <summary>
  ///   Get the correct taskId based on the SubmitId
  /// </summary>
  /// <param name="submitId">The submit Id push during the submission</param>
  /// <returns>the async taskId</returns>
  public async Task<string> GetResponseAsync(Guid submitId)
  {
    while (!dictionary_.ContainsKey(submitId))
    {
      await Task.Delay(WaitTime);
    }

    if (dictionary_[submitId]
          .TaskId == null && dictionary_[submitId]
          .Exception != null)
    {
      throw dictionary_[submitId]
        .Exception;
    }

    return dictionary_[submitId]
      .TaskId;
  }


  /// <summary>
  ///   Notice user that there was at least one error during the submission of buffer
  /// </summary>
  /// <param name="submitIds"></param>
  /// <param name="exception">exception occurring the submission</param>
  /// <exception cref="NotImplementedException"></exception>
  public void BufferFailures(IEnumerable<Guid> submitIds,
                             Exception         exception)
  {
    foreach (var submitId in submitIds)
    {
      dictionary_[submitId] = new RequestMapValue(exception);
    }
  }

  private struct RequestMapValue
  {
    public readonly string    TaskId;
    public readonly Exception Exception;

    public RequestMapValue(string taskId)
    {
      TaskId    = taskId;
      Exception = null;
    }

    public RequestMapValue(Exception exception)
    {
      TaskId    = null;
      Exception = exception;
    }
  }
}
