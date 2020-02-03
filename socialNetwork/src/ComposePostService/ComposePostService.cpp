#include <signal.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>

#include "../utils.h"
#include "ComposePostHandler.h"

using apache::thrift::protocol::TBinaryProtocolFactory;
using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::transport::TServerSocket;
using namespace social_network;

void sigintHandler(int sig) {
  exit(EXIT_SUCCESS);
}

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();
  SetUpTracer("config/jaeger-config.yml", "compose-post-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["compose-post-service"]["port"];

  int redis_port = config_json["compose-post-redis"]["port"];
  std::string redis_addr = config_json["compose-post-redis"]["addr"];
  int redis_connections = config_json["compose-post-redis"]["connections"];
  int redis_timeout_ms = config_json["compose-post-redis"]["timeout_ms"];

  int rabbitmq_port = config_json["write-home-timeline-rabbitmq"]["port"];
  std::string rabbitmq_addr = config_json["write-home-timeline-rabbitmq"]["addr"];
  int rabbitmq_connections = config_json["write-home-timeline-rabbitmq"]["connections"];
  int rabbitmq_timeout_ms = config_json["write-home-timeline-rabbitmq"]["timeout_ms"];

  int post_storage_port = config_json["post-storage-service"]["port"];
  std::string post_storage_addr = config_json["post-storage-service"]["addr"];
  int post_storage_connections = config_json["post-storage-service"]["connections"];
  int post_storage_timeout_ms = config_json["post-storage-service"]["timeout_ms"];

  int user_timeline_port = config_json["user-timeline-service"]["port"];
  std::string user_timeline_addr = config_json["user-timeline-service"]["addr"];
  int user_timeline_connections = config_json["user-timeline-service"]["connections"];
  int user_timeline_timeout_ms = config_json["user-timeline-service"]["timeout_ms"];

  ClientPool<RedisClient> redis_client_pool("redis", redis_addr, redis_port, 0, redis_connections, redis_timeout_ms);
  ClientPool<ThriftClient<PostStorageServiceClient>> post_storage_client_pool("post-storage-client", post_storage_addr, post_storage_port, 0, post_storage_connections, post_storage_timeout_ms);
  ClientPool<ThriftClient<UserTimelineServiceClient>> user_timeline_client_pool("user-timeline-client", user_timeline_addr, user_timeline_port, 0, user_timeline_connections, user_timeline_timeout_ms);
  ClientPool<RabbitmqClient> rabbitmq_client_pool("rabbitmq", rabbitmq_addr, rabbitmq_port, 0, rabbitmq_connections, rabbitmq_timeout_ms);

  TThreadedServer server(
      std::make_shared<ComposePostServiceProcessor>(
          std::make_shared<ComposePostHandler>(
              &redis_client_pool,
              &post_storage_client_pool,
              &user_timeline_client_pool,
              &rabbitmq_client_pool)),
      std::make_shared<TServerSocket>("0.0.0.0", port),
      std::make_shared<TFramedTransportFactory>(),
      std::make_shared<TBinaryProtocolFactory>());
  std::cout << "Starting the compose-post-service server ..." << std::endl;
  server.serve();
}
