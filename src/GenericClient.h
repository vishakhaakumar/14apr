#ifndef MOVIES_MICROSERVICES_GENERICCLIENT_H
#define MOVIES_MICROSERVICES_GENERICCLIENT_H

#include <string>

namespace movies {

class GenericClient{
 public:
  virtual ~GenericClient() = default;
  virtual void Connect() = 0;
  virtual void KeepAlive() = 0;
  virtual void KeepAlive(int) = 0;
  virtual void Disconnect() = 0;
  virtual bool IsConnected() = 0;

 protected:
  std::string _addr;
  int _port;
};

} // namespace movies

#endif //MOVIES_MICROSERVICES_GENERICCLIENT_H
