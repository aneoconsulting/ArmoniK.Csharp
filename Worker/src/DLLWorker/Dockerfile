#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
WORKDIR /app
ARG TARGETARCH


FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY ["Worker/src/DLLWorker/ArmoniK.DevelopmentKit.Worker.DLLWorker.csproj", "Worker/src/DLLWorker/"]
COPY ["Common/src/Common/ArmoniK.DevelopmentKit.Common.csproj", "Common/src/Common/"]
COPY ["Client/src/Common/ArmoniK.DevelopmentKit.Client.Common.csproj", "Client/src/Common/"]
COPY ["Worker/src/Common/ArmoniK.DevelopmentKit.Worker.Common.csproj", "Worker/src/Common/"]
RUN dotnet restore -a "${TARGETARCH}" "Worker/src/DLLWorker/ArmoniK.DevelopmentKit.Worker.DLLWorker.csproj"
COPY . .
WORKDIR "/src/Worker/src/DLLWorker"
ARG BUILD_CONFIG=Release
RUN dotnet build "ArmoniK.DevelopmentKit.Worker.DLLWorker.csproj" -c "${BUILD_CONFIG}" -f net6.0 -o /app/build

FROM build AS publish
RUN dotnet publish -a "${TARGETARCH}" "ArmoniK.DevelopmentKit.Worker.DLLWorker.csproj" -c "${BUILD_CONFIG}" -f net6.0 -o /app/publish --self-contained true

FROM base AS final

COPY ["Worker/src/DLLWorker/entry.sh", "/entry.sh"]

WORKDIR /app
COPY --from=publish /app/publish .
ENV LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH
RUN groupadd --gid 5000 armonikuser && useradd --home-dir /home/armonikuser --create-home --uid 5000 --gid 5000 --shell /bin/sh --skel /dev/null armonikuser
RUN mkdir /cache /data && chown armonikuser: /cache /data
USER armonikuser

ENTRYPOINT ["/entry.sh", "dotnet", "ArmoniK.DevelopmentKit.Worker.DLLWorker.dll"]
