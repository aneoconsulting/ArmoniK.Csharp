ARG WORKER_DLL_IMAGE=dockerhubaneo/armonik_worker_dll:0.14.5

FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY . .


WORKDIR "/src/Tests/ArmoniK.EndToEndTests/ArmoniK.EndToEndTests.Worker"

RUN dotnet publish -a "${TARGETARCH}" --self-contained -c Release -f net6.0 .


FROM ${WORKER_DLL_IMAGE} AS final

ENV ServiceAssemblyPath=/guest
ENV FileStorageType=FS
COPY --from=build /src/Tests/ArmoniK.EndToEndTests/publish/ArmoniK.EndToEndTests.Worker/1.0.0-700/ /guest
