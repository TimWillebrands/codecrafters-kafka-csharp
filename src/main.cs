using System.Buffers;
using System.Net;
using System.Net.Sockets;
using CodecraftersKafka;

Console.WriteLine("Logs from your program will appear here!");
var server = new TcpListener(IPAddress.Any, 9092);
var cts = new CancellationTokenSource();


Console.CancelKeyPress += (sender, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

try
{
    server.Start();
    Console.WriteLine("Server started. Press Ctrl+C to stop.");

    while (!cts.Token.IsCancellationRequested)
    {
        var client = await server.AcceptTcpClientAsync(cts.Token);
        _ = Main.HandleClientAsync(client, cts.Token).ContinueWith(task =>
        {
            if (task.IsFaulted)
                Console.WriteLine($"Client error: {task.Exception}");
            client.Dispose();
        }, cts.Token);
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Server stopped.");
}
finally
{
    server.Stop();
}

internal class Main
{
    private static ClusterMetadataLog ClusterMetadata = new ();

    internal static async Task HandleClientAsync(TcpClient client, CancellationToken ct)
    {
        await using var stream = client.GetStream();
        var pool = ArrayPool<byte>.Shared;

        while (!ct.IsCancellationRequested)
        {
            var buffer = pool.Rent(1024);
            try
            {
                var received = await stream.ReadAsync(buffer, ct);
                if (received == 0) break; // Client disconnected

                var request = Request.FromSpan(new ArraySegment<byte>(buffer, 0, received));

                Console.WriteLine($"Request: {request}");

                var isApiVersionUnsupported = request.Header.ApiKeyVersion is <= 0 or > 4;

                IKafkaResponse response = request switch
                {
                    ApiVersionsReqBody =>
                        isApiVersionUnsupported
                            ? new KafkaResponse<ApiVersionsBody>(
                                new ResponseHeader(request.Header.CorrelationId),
                                new ApiVersionsBody(ErrorCode.UnsupportedVersion, []))
                            : new KafkaResponse<ApiVersionsBody>(
                                new ResponseHeader(request.Header.CorrelationId),
                                new ApiVersionsBody(ErrorCode.None, [
                                    new ApiVersion(ApiKey.ApiVersions, 0, 4),
                                    new ApiVersion(ApiKey.DescribeTopicPartitions, 0, 0),
                                    new ApiVersion(ApiKey.Fetch, 0, 16),
                                ])),
                    DescribeTopicPartitionsReqBody req =>
                        new KafkaResponse<DescribeTopicPartitionsBody>(
                            new ResponseHeader(req.Header.CorrelationId),
                            new DescribeTopicPartitionsBody(req, ClusterMetadata)),
                    _ => throw new ArgumentOutOfRangeException($"We dont support {request.Header.ApiKey}")
                };

                Console.WriteLine($"Response: {response}");

                await stream.WriteAsync(response.ToSpan().ToArray(), ct);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            finally
            {
                pool.Return(buffer);
            }
        }
    }
}