using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Proto.Remote.Tests.Messages;

namespace Proto.Remote.Tests
{
    public class EchoActor : IActor
    {
        private static readonly ILogger Logger = Log.CreateLogger<EchoActor>();

        public async Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Started:
                    Logger.LogDebug($"{context.Self}");
                    break;
                case Ping ping:
                    Logger.LogDebug("Received Ping, replying Pong");
                    context.Respond(new Pong { Message = $"{context.System.Address} {ping.Message}" });
                    break;
                case BinaryMessage msg:
                    Logger.LogDebug("Received BinaryMessage, replying Ack");
                    context.Respond(new Ack());
                    break;
                case Die:
                    Logger.LogDebug("Received termination request, stopping");
                    context.Stop(context.Self);
                    break;
                case SpawnOnMeAndPing:
                    var remoteActorName = Guid.NewGuid().ToString();
                    var sender = context.Sender;
                    if (context.Sender.TryTranslateToLocalClientPID(out var translatedPid))
                    {
                        sender = translatedPid;
                    }
                    var remoteActorResp = await context.System.Remote().SpawnNamedAsync(
                        sender.Address, remoteActorName, "EchoActor", TimeSpan.FromSeconds(5)
                    );
                    var remoteActor = remoteActorResp.Pid;
                    var pong = await context.RequestAsync<Pong>(remoteActor, new Ping { Message = "Hello" },
                        TimeSpan.FromMilliseconds(5000)
                    );
                    context.Respond(new SpawnOnMeAndPingResponse { Pid = remoteActor, Message = pong.Message });
                    break;
                default:
                    Logger.LogDebug(context.Message.GetType().Name);
                    break;
            }
        }
    }
}