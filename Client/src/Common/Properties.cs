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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.DevelopmentKit.Common;

using Google.Protobuf.WellKnownTypes;

using Microsoft.Extensions.Configuration;

namespace ArmoniK.DevelopmentKit.Client.Common;

/// <summary>
///   The properties class to set all configuration of the service
///   1. The connection information
///   2. The Option configuration AppSettings
///   The ssl mTLS certificate if needed to connect to the control plane
/// </summary>
[MarkDownDoc]
public class Properties
{
  /// <summary>
  ///   The default configuration to submit task in a Session
  /// </summary>
  public static TaskOptions DefaultTaskOptions = new()
                                                 {
                                                   MaxDuration = new Duration
                                                                 {
                                                                   Seconds = 300000,
                                                                 },
                                                   MaxRetries = 3,
                                                   Priority   = 1,
                                                 };


  /// <summary>
  ///   The constructor to instantiate Properties object
  /// </summary>
  /// <param name="options">The taskOptions to set to a session</param>
  /// <param name="connectionAddress">The control plane address to connect</param>
  /// <param name="connectionPort">The optional port to connect to the control plane</param>
  /// <param name="protocol">the protocol https or http</param>
  /// <param name="clientCertPem">The client certificate fil in a pem format</param>
  /// <param name="clientKeyPem">The client key file in a pem format</param>
  /// <param name="caCertPem">The Server certificate file to validate mTLS</param>
  /// <param name="sslValidation">Disable the ssl strong validation of ssl certificate (default : enable => true)</param>
  public Properties(TaskOptions options,
                    string      connectionAddress,
                    int         connectionPort = 0,
                    string      protocol       = null,
                    string      clientCertPem  = null,
                    string      clientKeyPem   = null,
                    string      caCertPem      = null,
                    bool        sslValidation  = true)
    : this(new ConfigurationBuilder().AddEnvironmentVariables()
                                     .Build(),
           options,
           connectionAddress,
           connectionPort,
           protocol,
           clientCertPem,
           clientKeyPem,
           caCertPem,
           sslValidation)
  {
  }

  /// <summary>
  ///   The constructor to instantiate Properties object
  /// </summary>
  /// <param name="configuration">The configuration to read information from AppSettings file</param>
  /// <param name="options">The taskOptions to set to a session</param>
  /// <param name="connectionAddress">The control plane address to connect</param>
  /// <param name="connectionPort">The optional port to connect to the control plane</param>
  /// <param name="protocol">the protocol https or http</param>
  /// <param name="caCertPem">The Server certificate file to validate mTLS</param>
  /// <param name="clientCertFilePem">The client certificate fil in a pem format</param>
  /// <param name="clientKeyFilePem">The client key file in a pem format</param>
  /// <param name="sslValidation">Disable the ssl strong validation of ssl certificate (default : enable => true)</param>
  /// <exception cref="ArgumentException"></exception>
  public Properties(IConfiguration configuration,
                    TaskOptions    options,
                    string         connectionAddress = null,
                    int            connectionPort    = 0,
                    string         protocol          = null,
                    string         clientCertFilePem = null,
                    string         clientKeyFilePem  = null,
                    string         caCertPem         = null,
                    bool           sslValidation     = true)
  {
    TaskOptions   = options;
    Configuration = configuration;

    var sectionGrpc = configuration.GetSection(SectionGrpc)
                                   .Exists()
                        ? configuration.GetSection(SectionGrpc)
                        : null;


    if (sectionGrpc != null)
    {
      ConnectionString = sectionGrpc.GetSection(SectionEndPoint)
                                    .Exists()
                           ? sectionGrpc[SectionEndPoint]
                           : null;
      ConfSSLValidation = !sectionGrpc.GetSection(SectionSSlValidation)
                                      .Exists() || sectionGrpc[SectionSSlValidation] != "disable";

      if (sectionGrpc.GetSection(SectionMTls)
                     .Exists() && sectionGrpc[SectionMTls]
            .ToLower() == "true")
      {
        CaCertFilePem = sectionGrpc.GetSection(SectionCaCert)
                                   .Exists()
                          ? sectionGrpc[SectionCaCert]
                          : null;
        ClientCertFilePem = sectionGrpc.GetSection(SectionClientCert)
                                       .Exists()
                              ? sectionGrpc[SectionClientCert]
                              : null;
        ClientKeyFilePem = sectionGrpc.GetSection(SectionClientKey)
                                      .Exists()
                             ? sectionGrpc[SectionClientKey]
                             : null;
      }
    }

    if (clientCertFilePem != null)
    {
      ClientCertFilePem = clientCertFilePem;
    }

    if (clientKeyFilePem != null)
    {
      ClientKeyFilePem = clientKeyFilePem;
    }

    if (caCertPem != null)
    {
      CaCertFilePem = caCertPem;
    }

    ConfSSLValidation = sslValidation && ConfSSLValidation;

    //Console.WriteLine($"Parameters coming from Properties :\n" +
    //                  $"ConnectionString  = {ConnectionString}\n" +
    //                  $"ConfSSLValidation = {ConfSSLValidation}\n" +
    //                  $"CaCertFilePem     = {CaCertFilePem}\n" +
    //                  $"ClientCertFilePem = {ClientCertFilePem}\n" +
    //                  $"ClientKeyFilePem  = {ClientKeyFilePem}\n"
    //                  );

    if (connectionAddress != null)
    {
      var uri = new Uri(connectionAddress);
      ConnectionAddress = uri.Host;

      if (!string.IsNullOrEmpty(uri.Scheme))
      {
        Protocol = uri.Scheme;
      }
    }

    if (connectionPort != 0)
    {
      ConnectionPort = connectionPort;
    }

    if (protocol != null)
    {
      Protocol = protocol;
    }

    //Check if Uri is correct
    if (Protocol == "err://" || ConnectionAddress == "NoEndPoint" || ConnectionPort == 0)
    {
      throw new ArgumentException($"Issue with the connection point : {ConnectionString}");
    }

    ControlPlaneUri = new Uri(ConnectionString);
  }

  /// <summary>
  ///   Set the number of task by buffer
  /// </summary>
  public int MaxTasksPerBuffer { get; set; } = 500;


  /// <summary>
  ///   Set the number of buffers that can be filled in asynchronous submitAsync
  /// </summary>
  public int MaxConcurrentBuffers { get; set; } = 1;


  /// <summary>
  ///   TimeSpan to trigger a batch to send the batch of submit
  /// </summary>
  public TimeSpan? TimeTriggerBuffer { get; set; } = TimeSpan.FromSeconds(10);

  /// <summary>
  ///   The number of channels used for Buffered Submit (Default 1)
  /// </summary>
  public int MaxParallelChannels { get; set; } = 1;

  /// <summary>
  ///   The control plane url to connect
  /// </summary>
  public Uri ControlPlaneUri { get; set; }

  /// <summary>
  ///   Returns the section key Grpc from appSettings.json
  /// </summary>
  private static string SectionGrpc { get; } = "Grpc";

  private static string SectionEndPoint      { get; } = "EndPoint";
  private static string SectionSSlValidation { get; } = "SSLValidation";
  private static string SectionCaCert        { get; } = "CaCert";
  private static string SectionClientCert    { get; } = "ClientCert";
  private static string SectionClientKey     { get; } = "ClientKey";

  /// <summary>
  ///   The key to select mTls in configuration
  /// </summary>
  public string SectionMTls { get; set; } = "mTLS";

  /// <summary>
  ///   The path to the CA Root file name
  /// </summary>
  public string CaCertFilePem { get; set; }

  /// <summary>
  ///   The property to get the path of the certificate file
  /// </summary>
  public string ClientCertFilePem { get; }

  /// <summary>
  ///   the property to get the path of the key certificate
  /// </summary>
  public string ClientKeyFilePem { get; }

  /// <summary>
  ///   The SSL validation property to disable SSL strong verification
  /// </summary>
  public bool ConfSSLValidation { get; }

  /// <summary>
  ///   The configuration property to give to the ClientService connector
  /// </summary>
  public IConfiguration Configuration { get; }

  /// <summary>
  ///   The connection string building the value Port Protocol and address
  /// </summary>
  public string ConnectionString
  {
    get => $"{Protocol}://{ConnectionAddress}:{ConnectionPort}";
    set
    {
      try
      {
        if (string.IsNullOrEmpty(value))
        {
          return;
        }

        var uri = new Uri(value);

        Protocol = uri.Scheme;

        ConnectionAddress = uri.Host;
        ConnectionPort    = uri.Port;
      }
      catch (FormatException e)
      {
        Console.WriteLine(e);
        throw;
      }
    }
  }

  /// <summary>
  ///   Secure or insecure protocol communication https or http (Default http)
  /// </summary>
  public string Protocol { get; set; } = "http";

  /// <summary>
  ///   The connection address property to connect to the control plane
  /// </summary>
  public string ConnectionAddress { get; set; }

  /// <summary>
  ///   The option connection port to connect to control plane (Default : 5001)
  /// </summary>
  public int ConnectionPort { get; set; } = 5001;

  /// <summary>
  ///   The TaskOptions to pass to the session or the submission session
  /// </summary>
  public TaskOptions TaskOptions { get; set; }

  /// <summary>
  ///   Gets or sets the maximum number of retries. Default 5 retries
  /// </summary>
  public static int MaxRetries { get; set; } = 5;

  /// <summary>
  ///   Gets or sets the time interval between retries. Default 2000 ms
  /// </summary>
  public static int TimeIntervalRetriesInMs { get; set; } = 2000;
}
