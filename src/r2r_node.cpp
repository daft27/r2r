#define ZMQ_BUILD_DRAFT_API
#include <zmq.hpp>
#include <ros/ros.h>

#include <ros/master.h>

#include <msgpack.hpp>

#include <iostream>
#include <fstream>

#include "json.hpp"

#include <limits.h>

#include <std_msgs/Int32.h>
#include <sensor_msgs/NavSatFix.h>
#include <sensor_msgs/LaserScan.h>
#include <sensor_msgs/Imu.h>
#include <sensor_msgs/CompressedImage.h>
#include <geometry_msgs/Twist.h>
#include <nav_msgs/Path.h>
#include <nav_msgs/Odometry.h>

#include <memory>
#include <functional>



#define VERBOSE 0
#if VERBOSE
	#define OUT(x) (x)
#else
	#define OUT(x) ((void)0)
#endif


#define TYPE_CONVERSION(func, ...)  \
	if(type == "std_msgs/Int32") 		func<std_msgs::Int32>(__VA_ARGS__);\
	else if(type == "sensor_msgs/NavSatFix") 		func<sensor_msgs::NavSatFix>(__VA_ARGS__);\
	else if(type == "sensor_msgs/LaserScan")	func<sensor_msgs::LaserScan>(__VA_ARGS__);\
	else if(type == "sensor_msgs/Imu")		func<sensor_msgs::Imu>(__VA_ARGS__);\
	else if(type == "sensor_msgs/CompressedImage")	func<sensor_msgs::CompressedImage>(__VA_ARGS__);\
	else if(type == "geometry_msgs/Twist") 		func<geometry_msgs::Twist>(__VA_ARGS__);\
	else if(type == "nav_msgs/Odometry")		func<nav_msgs::Odometry>(__VA_ARGS__);\
	else if(type == "nav_msgs/Path")		func<nav_msgs::Path>(__VA_ARGS__);


const int kServiceBufferSize = 10;


template<typename T, typename ...Args>
std::unique_ptr<T> make_unique( Args&& ...args )
{
    return std::unique_ptr<T>( new T( std::forward<Args>(args)... ) );
}


using json = nlohmann::json;

struct ServiceList 
{
	// publications
	std::vector<std::string> 	topics;
	std::vector<std::string> 	types;
	std::vector<int> 		qoss;

	// source
	std::string 			host;
	int				time_s;
	int				time_ns;

	int				bytes_sent;

	int				bytes_sent_qos1;
	int				bytes_sent_qos2;
	int				bytes_sent_qos3;

	int				bytes_recv;
	int				bytes_recv_qos1;
	int				bytes_recv_qos2;
	int				bytes_recv_qos3;
	MSGPACK_DEFINE(topics, types, qoss, host, time_s, time_ns, bytes_sent, bytes_sent_qos1, bytes_sent_qos2, bytes_sent_qos3);
};



struct MessageContainer
{
	std::string 			host;
	std::string 			topic;
	std::string			type;
	std::vector<unsigned char> 	payload;	

	MessageContainer() = default;


	template<typename T>
	MessageContainer(const std::string & topic, const std::string & host, const T & data) : topic(topic), host(host), type(ros::message_traits::DataType<T>::value()), payload(ros::serialization::serializationLength(data))
	{
		namespace ser = ros::serialization;

		ser::OStream stream(&payload[0], payload.size());
		ser::Serializer<T>::write(stream, data);
	}

	void DebugPrint() const
	{
		std::cout << "\thost : " << host << std::endl;
		std::cout << "\ttopic : " << topic << std::endl;
		std::cout << "\ttype : " << type<< std::endl;
		std::cout << "\tpayload : " << payload.size() << std::endl;
	}

	template<typename T>
	T To() const
	{
		namespace ser = ros::serialization;
		 
		T my_value;
		 
		ser::IStream stream(const_cast<unsigned char*>(&payload[0]), payload.size());
		ser::Serializer<T>::read(stream, my_value);

		return my_value;
	}

	MSGPACK_DEFINE(host, topic, type, payload);
};

using ForwardingCallback = void(const MessageContainer &, int qos);
using SubscriberCheckCallback = void(const std::string & host, const std::string & topic, int change, int current, int qoslvl);

// listen to topics that need to be published elsewhere
class LocalExchangeServer
{

	struct Topic
	{
		ros::Subscriber sub;
		ros::Publisher	pub;
		std::string	type;
		int		qoslvl = 0;
		int		num_local_subscribers = 0;
	};

	ros::NodeHandle						nh_;
	std::string 						host_;
	std::map<std::string, Topic> 				topics_;
	std::map<std::string, std::map<std::string, Topic>> 	remote_topics_;
	std::map<std::string, ros::Publisher>			remote_reception_pubs_;

	std::function<ForwardingCallback> 			forwardFunc_ = nullptr;

	void Forward(const MessageContainer & mc, int qos)
	{
		if(forwardFunc_) {
			OUT(std::cout << " [LES] Forwarding " << mc.type << std::endl);
			forwardFunc_(mc, qos);
		}
	}

	template<typename T>
	void ROSMsgCallback(const typename T::ConstPtr & msg, const std::string & topic)
	{
		static bool reportedWarn = false;
		if(reportedWarn == false)  {
			std::cerr << " [LES] Generic ROS message callback for " << topic << ", type " <<  ros::message_traits::DataType<T>::value() << ", LEN " << sizeof(*msg) << " Simple? " << ros::message_traits::IsSimple<T>::value << ", FixedSize? " << ros::message_traits::IsFixedSize<T>::value << std::endl;	
			reportedWarn = true;
		}

		MessageContainer mc = {topic, host_, *msg};

		int qos = topics_[topic].qoslvl;

		OUT(std::cout << " [LES] Recv from ROS " << topic << std::endl);

		Forward(mc, qos);
	}

	template<typename T>
	void SubscribeToLocalRosTopic(const std::string & topic)
	{
		try 
		{
			auto & t = topics_.at(topic);

			if(t.sub == nullptr) {
				std::cout << " [LES] subscribing to " << topic << std::endl;
				auto sub = nh_.subscribe<T>(topic, 1, boost::bind(&LocalExchangeServer::ROSMsgCallback<T>, this, _1, topic));
				t.sub = sub;
			}
		}
		catch (std::exception e)
		{
			std::cerr << "Failed to subscribe to local topic: " << topic << " (" << e.what() << ")" << std::endl;
		}
	}

	template<typename T>
	void PublishRemoteRosTopic(const std::string & topic, const std::string & host, const int qoslvl)
	{
		try 
		{ 
			auto & t = remote_topics_[host][topic];
	
			if(t.pub == nullptr) {
				std::cout << " [LES] publishing " << topic << " from " << host << std::endl;
				auto pub = nh_.advertise<T>(host + topic, 5);
				t.pub = pub;
				t.qoslvl = qoslvl;
			}
		} 
		catch (std::exception e) 
		{
			std::cerr << "Failed to publish remote topic: " << host << topic << " (" << e.what() << ")" << std::endl;
		}
	}


public:

	LocalExchangeServer(ros::NodeHandle nh, const std::string & host) : nh_(nh), host_(host) {}

	std::string GetHost() const { return host_; }

	void SetForward(std::function<ForwardingCallback> f) {	forwardFunc_ = f; }

	void SubscriberCheck(std::function<SubscriberCheckCallback> f)
	{
		for(auto & host_it : remote_topics_) {
			std::string host = host_it.first;

			for(auto & it : host_it.second) {

				auto & t = it.second;

				if(t.pub) {
					int difference = t.pub.getNumSubscribers() - t.num_local_subscribers;

					if(difference != 0) {
						// update and fire callback
						t.num_local_subscribers += difference;
						std::cout << " [LES] Subscriber change at /" << host << it.first << ". " << difference << ", now " << t.num_local_subscribers << std::endl;
						f(host_it.first, it.first, difference, t.num_local_subscribers, t.qoslvl);
					}
				}
			}
		}
	}

	void AddPublishedTopic(const std::string & topic, int qoslvl)
	{
		Topic t;
		t.qoslvl = qoslvl;
		topics_[topic] = t;
	}

	void PublishRemoteReception(const std::string & host, int qoslvl)
	{
		auto & pub = remote_reception_pubs_[host];

		if (pub == nullptr) {
			pub = nh_.advertise<std_msgs::Int32>(host + "/reception", 2, true);
			OUT(std::cout << " [LES] New reception publisher at " << host << "/reception" << std::endl);
		}

		std_msgs::Int32 msg;
		msg.data = qoslvl;

		pub.publish(msg);
	}

	void DebugPrint()
	{
		for(auto it : topics_) {
			std::cout << it.first << " (" << it.second.qoslvl << ")" << std::endl;
		}
	}


	void SubscribeToLocalRosTopic(const std::string & topic, const std::string & type)
	{
		// map type strings to C++ types
		TYPE_CONVERSION(SubscribeToLocalRosTopic, topic);
	}



	void PublishRemoteRosTopic(const std::string & topic, const std::string & type, const std::string & host, const int qoslvl)
	{
		// map type strings to C++ types
		TYPE_CONVERSION(PublishRemoteRosTopic, topic, host, qoslvl);
	}


	void SendToROS(const MessageContainer & mc)
	{
		OUT(std::cout << " [LES] Relayed to ROS " << mc.host << "/" << mc.topic << std::endl);

		const auto & type = mc.type;
		const auto & host = mc.host;
		const auto & topic = mc.topic;

		// map type strings to C++ types
		TYPE_CONVERSION(SendROSMessage, topic, host, mc);
	}

	template<typename T>
	void SendROSMessage(const std::string & topic, const std::string & host, const MessageContainer &mc)
	{
		try 
		{
			auto rt = remote_topics_.find(host);
			if(rt != remote_topics_.end()) {
				auto t = rt->second.find(topic);
				if(t !=  rt->second.end() && t->second.pub) 
					t->second.pub.publish(mc.To<T>());
			}
			
		} 
		catch (std::exception e)
		{
			std::cerr << "Failed to send ROS message. " << e.what() << std::endl;
		}
	}
};



class RemoteExchangeServer
{
	zmq::context_t context_;

	std::unique_ptr<zmq::socket_t> 	socket_ctrl_pub_;
	std::unique_ptr<zmq::socket_t> 	socket_ctrl_sub_;

	std::unique_ptr<zmq::socket_t> 	socket_radio_;
	std::unique_ptr<zmq::socket_t> 	socket_dish_;

	std::unique_ptr<zmq::socket_t> 	socket_bulk_pub_;
	std::unique_ptr<zmq::socket_t> 	socket_bulk_sub_;

	std::set<std::string>	 	bulk_connections_;

	int				msgs_sent_qos1_ = 0;
	int				msgs_sent_qos2_ = 0;
	int				msgs_sent_qos3_ = 0;

	int				bytes_sent_qos1_ = 0;
	int				bytes_sent_qos2_ = 0;
	int				bytes_sent_qos3_ = 0;

	int				bytes_recv_ = 0;
	int				bytes_sent_ = 0;

	std::function<ForwardingCallback> forwardFunc_ = nullptr;

	std::map<std::string, std::deque<ServiceList> > services_;


	void SubscribeToBulkRemote(const std::string & host)
	{
		if(bulk_connections_.find(host) == bulk_connections_.end()) {
			std::string destination = std::string("tcp://") + host + ":6667";
			socket_bulk_sub_->connect(destination.c_str());
			bulk_connections_.insert(host);
			std::cout << " [RES] Bulk subscribing to remote at " << destination << std::endl;
		} 
	}

	void Forward(const MessageContainer & mc, int qos)
	{
		if(forwardFunc_) {
			OUT(std::cout << " [RES] Forwarding " << mc.type << std::endl);
			forwardFunc_(mc, qos);

		}
	}

	void SetupDiscovery(const std::string & broadcast_addr, const std::string & broadcast_localaddr)
	{
		std::cout << " [RES] Setup discovery publisher" << std::endl;
		// make publisher
		socket_ctrl_pub_ = make_unique<zmq::socket_t>(context_, ZMQ_PUB);
		{
			// see https://tools.ietf.org/html/rfc791
			int tos = 0x28 << 2; // CS5 (DSCP 101 000) or 40
			socket_ctrl_pub_->setsockopt(ZMQ_TOS, &tos, sizeof(tos));
		//	int64_t rate = 1024;
		//	socket_ctrl_pub_->setsockopt(ZMQ_RATE, &rate, sizeof(rate));
		}
		std::cout << " [RES]    at " << broadcast_addr << std::endl;
		socket_ctrl_pub_->bind(broadcast_addr.c_str());
		std::cout << " [RES]    at " << broadcast_localaddr << std::endl;
		socket_ctrl_pub_->bind(broadcast_localaddr.c_str());

		std::cout << " [RES] Setup discovery subscriber" << std::endl;
		// make subscriber
		socket_ctrl_sub_ = make_unique<zmq::socket_t>(context_, ZMQ_SUB);
		socket_ctrl_sub_->setsockopt(ZMQ_SUBSCRIBE,"", 0);
		std::cout << " [RES]    at " << broadcast_addr << std::endl;
		socket_ctrl_sub_->connect(broadcast_addr.c_str());
		std::cout << " [RES]    at " << broadcast_localaddr << std::endl;
		socket_ctrl_sub_->connect(broadcast_localaddr.c_str());
	}

	void SetupMulticast()
	{
		std::string addr = "udp://239.192.1.1:6666";

		std::cout << " [RES] Setup multicast publisher" << std::endl;	
		socket_radio_ = make_unique<zmq::socket_t>(context_, ZMQ_RADIO);

		std::cout << " [RES]    at " << addr << std::endl;
		socket_radio_->connect(addr);
		{
			// see https://tools.ietf.org/html/rfc791
			int tos = 0x2e << 2; // EF (DSCP 101 110) or 46
			socket_radio_->setsockopt(ZMQ_TOS, &tos, sizeof(tos));
		}

		std::cout << " [RES] Setup multicast subscriber" << std::endl;	
		socket_dish_ = make_unique<zmq::socket_t>(context_, ZMQ_DISH);

		std::cout << " [RES]    at " << addr << std::endl;
		socket_dish_->bind(addr);
		zmq_join((void*) *(socket_dish_.get()), "");
	}

	void SetupBulk(const bool receiveAll)
	{
		std::string addr = "tcp://*:6667";
		std::cout << " [RES] Setup bulk" << std::endl;

		socket_bulk_pub_ = make_unique<zmq::socket_t>(context_, ZMQ_PUB);
		std::cout << " [RES]    at " << addr << std::endl;
		socket_bulk_pub_->bind(addr);
		{
			int limit = 2;
			socket_bulk_pub_->setsockopt(ZMQ_SNDHWM, &limit, sizeof(limit));

			uint64_t buffer = 256 * 1024;
			socket_bulk_pub_->setsockopt(ZMQ_SNDBUF, &limit, sizeof(limit));

		}

		socket_bulk_sub_ = make_unique<zmq::socket_t>(context_, ZMQ_SUB);

		if (receiveAll)
			socket_bulk_sub_->setsockopt(ZMQ_SUBSCRIBE,"", 0);
	}

	void RecordBytesRecv(int bytes, const std::string & host, int qoslvl)
	{
		bytes_recv_+= bytes;

		auto serviceListsIter = services_.find(host);
		if (serviceListsIter != services_.end()) {
			auto &lastSL = serviceListsIter->second.back();
			lastSL.bytes_recv += bytes;

			switch(qoslvl) {
				case 0: lastSL.bytes_recv_qos1 += bytes; break;
				case 1: lastSL.bytes_recv_qos2 += bytes; break;
				case 2: lastSL.bytes_recv_qos3 += bytes; break;
			}
		}
		
	}

	int GetReceptionLevel(const std::string & host) const
	{
		auto serviceListsIter = services_.find(host);
		if (serviceListsIter != services_.end()) {
			const auto & list = serviceListsIter->second;

			const int listSize = list.size();
			if (listSize >= 2) {

				const auto & last1 = list[listSize-1];
				const auto & last2 = list[listSize-2];

				int lastSentBytesQOS1 = last1.bytes_sent_qos1 - last2.bytes_sent_qos1;
				int lastSentBytesQOS2 = last1.bytes_sent_qos2 - last2.bytes_sent_qos2;
				int lastSentBytesQOS3 = last1.bytes_sent_qos3 - last2.bytes_sent_qos3;

				int lastRecvBytesQOS1 = last1.bytes_recv_qos1;
				int lastRecvBytesQOS2 = last1.bytes_recv_qos2;
				int lastRecvBytesQOS3 = last1.bytes_recv_qos3;
				
				double qos1P = lastRecvBytesQOS1 ?  ((double) lastSentBytesQOS1 / lastRecvBytesQOS1) : 1;
				double qos2P = lastRecvBytesQOS2 ?  ((double) lastSentBytesQOS2 / lastRecvBytesQOS2) : 1;
				double qos3P = lastRecvBytesQOS3 ?  ((double) lastSentBytesQOS3 / lastRecvBytesQOS3) : 1;

				return (int) ceil(qos1P * 30 + qos2P * 50 + qos3P * 20);
			}

		}

		return -1;
	}

public:

	RemoteExchangeServer() : context_(1) { }

	int GetBytesSent() const { return bytes_sent_; }
	int GetBytesRecv() const { return bytes_recv_; }

	void Setup(const std::string & broadcast_addr, const std::string & broadcast_localaddr, bool receiveAll)
	{
		SetupDiscovery(broadcast_addr, broadcast_localaddr);
		SetupMulticast();
		SetupBulk(receiveAll);
	}


	void SendServiceBroadcast(const ServiceList & sl)
	{
		msgpack::sbuffer buffer;
		msgpack::pack(&buffer, sl);

		zmq::message_t msg (buffer.size());
		memcpy (msg.data (), buffer.data(), buffer.size());

		if(socket_ctrl_pub_->send (msg))
			OUT(std::cout << " [EPGM] Sent " << buffer.size() << std::endl);
		else
			std::cerr << " [EPGM] DROPPED " << buffer.size() << std::endl;


		msgs_sent_qos1_++;
		bytes_sent_qos1_ += buffer.size();
		bytes_sent_+= buffer.size();
	}


	using ServiceCallback = void(const std::string & topic, const std::string & type, const std::string & host, const int qoslvl);
	using ServiceCallback2 = void(const std::string & host, int qoslvl);

	void RecvServiceBroadcast(std::function<ServiceCallback> f = nullptr, std::function<ServiceCallback2> f2 = nullptr)
	{
		zmq::message_t request;
		if(socket_ctrl_sub_->recv (&request, ZMQ_DONTWAIT)) {
			OUT(std::cout << " [EPGM] Recv " << request.size());

			msgpack::unpacked result;
			msgpack::unpack(&result, (const char*) request.data(), request.size());

			msgpack::object obj = result.get();
			ServiceList sl = obj.as<ServiceList>();

			OUT(std::cout << " from " << sl.host << std::endl);

			for(int i = 0 ; i < sl.topics.size(); i++ ) {
				// std::cout << " < " << sl.topics[i] << "<" << sl.types[i] << "> ( " << sl.qoss[i] << " )" << std::endl;
				if(f) {
					f(sl.topics[i], sl.types[i], sl.host, sl.qoss[i]);
				}
			}
			
			SubscribeToBulkRemote(sl.host);
			RecordBytesRecv(request.size(), sl.host, 0);

			int reception = GetReceptionLevel(sl.host);

			if(f2)
				f2(sl.host, reception);

			//PublishRemoteReception(sl.host, reception);
			OUT(std::cout << "   reception: " << reception << std::endl);

			// add to buffer
			sl.bytes_recv = 0;
			sl.bytes_recv_qos1 = 0;
			sl.bytes_recv_qos2 = 0;
			sl.bytes_recv_qos3 = 0;

			auto & serviceLists = services_[sl.host];
			serviceLists.push_back(sl);

			if (serviceLists.size() > kServiceBufferSize) {
				serviceLists.pop_front();
			}
		}

	}


	void SendMulticast(const MessageContainer & data)
	{
		msgpack::sbuffer buffer;
		msgpack::pack(&buffer, data);

		zmq::message_t msg (buffer.size());
		memcpy (msg.data (), buffer.data(), buffer.size());

		if(socket_radio_->send(msg))
			OUT(std::cout << " [MUDP] Sent " << buffer.size() << std::endl);
		else
			std::cerr << " [MUDP] DROPPED " << buffer.size() << std::endl;

		msgs_sent_qos2_++;
		bytes_sent_qos2_ += buffer.size();
		bytes_sent_+= buffer.size();
	}

	void RecvMulticast()
	{
		zmq::message_t request;
		while(socket_dish_->recv (&request, ZMQ_DONTWAIT)) {
			OUT(std::cout << " [MUDP] Recv " << request.size() << std::endl);

			msgpack::unpacked result;
			msgpack::unpack(&result, (const char*) request.data(), request.size());

			msgpack::object obj = result.get();
			MessageContainer mc = obj.as<MessageContainer>();

			//mc.DebugPrint();

			Forward(mc, 1);
		
			RecordBytesRecv(request.size(), mc.host, 1);
		}
	}

	void SendBulk(const MessageContainer & data)
	{
		msgpack::sbuffer buffer;
		msgpack::pack(&buffer, data);

		const size_t topichash = std::hash<std::string>()(data.host + data.topic);
		const size_t topicsize = sizeof(size_t);

		zmq::message_t msg (buffer.size() + topicsize);
		memcpy (msg.data(), &topichash, topicsize);
		memcpy ((char*) msg.data() + topicsize, buffer.data(), buffer.size());

		if(socket_bulk_pub_->send(msg, ZMQ_NOBLOCK))
			OUT(std::cout << " [BTCP] Sent " << buffer.size() << std::endl);
		else
			std::cerr << " [BTCP] DROPPED " << buffer.size() << std::endl;

		msgs_sent_qos3_++;
		bytes_sent_qos3_ += buffer.size();
		bytes_sent_+= buffer.size();
	}

	void ChannelSubscribe(int qoslvl, size_t topic)
	{
		OUT(std::cout << "Subscribed" << std::endl);
		switch(qoslvl) 
		{
			case 2: break;
			case 3: socket_bulk_sub_->setsockopt(ZMQ_SUBSCRIBE, &topic, sizeof(topic)); break;
		}
	}

	void ChannelUnsubscribe(int qoslvl, size_t topic)
	{
		OUT(std::cout << "Unsubscribed" << std::endl);
		switch(qoslvl) 
		{
			case 2: break;
			case 3: socket_bulk_sub_->setsockopt(ZMQ_UNSUBSCRIBE, &topic, sizeof(topic)); break;
		}
	}


	void RecvBulk()
	{
		zmq::message_t request;
		while(socket_bulk_sub_->recv (&request, ZMQ_DONTWAIT)) {
			OUT(std::cout << " [BTCP] Recv " << request.size() << std::endl);

			const size_t topicsize = sizeof(size_t);

			msgpack::unpacked result;
			msgpack::unpack(&result, (const char*) request.data() + topicsize, request.size() - topicsize);

			msgpack::object obj = result.get();
			MessageContainer mc = obj.as<MessageContainer>();

			// mc.DebugPrint();

			Forward(mc, 2);

			RecordBytesRecv(request.size(), mc.host, 2);
		}
	}


	
	void SetForward(std::function<ForwardingCallback> f) { forwardFunc_ = f; }

	int GetMsgsSentQOS1() const { return msgs_sent_qos1_; }
	int GetMsgsSentQOS2() const { return msgs_sent_qos2_; }
	int GetMsgsSentQOS3() const { return msgs_sent_qos3_; }
	int GetBytesSentQOS1() const { return bytes_sent_qos1_; }
	int GetBytesSentQOS2() const { return bytes_sent_qos2_; }
	int GetBytesSentQOS3() const { return bytes_sent_qos3_; }

	void Poll(std::function<ServiceCallback> f)
	{
		zmq::pollitem_t items [] = {
		        { (void*) *socket_ctrl_sub_, 0, ZMQ_POLLIN, 0 },
		        { (void*) *socket_dish_, 0, ZMQ_POLLIN, 0 },
		        { (void*) *socket_bulk_sub_, 0, ZMQ_POLLIN, 0 }
		    };

		zmq::poll (&items [0], 3, 1000);

		if(items[0].revents & ZMQ_POLLIN) {
			RecvServiceBroadcast(f);
		}

		if(items[1].revents & ZMQ_POLLIN) {
			RecvMulticast();
		}

		if(items[2].revents & ZMQ_POLLIN) {
			RecvBulk();
		}
	}
};




int main(int argc, char ** argv)
{
	ros::init(argc, argv, "r2r");	
	ros::NodeHandle nh("~");

	std::string broadcast_addr = nh.param<std::string>("broadcast_addr", "epgm://eth0;239.192.1.1:5555");
	std::string broadcast_localaddr = nh.param<std::string>("broadcast_localaddr", "ipc:///tmp/localfeed0");
	std::string topics_file = nh.param<std::string>("topics_file","");
	bool self_subscribe = nh.param<bool>("self_subscribe", false);
	bool receive_all = nh.param<bool>("receive_all", false);

	char hostname[HOST_NAME_MAX];
	gethostname(hostname, sizeof(hostname));

	std::string host = nh.param<std::string>("hostname",hostname);

	std::cout<<"Broadcast_addr: " << broadcast_addr << std::endl;
	std::cout<<"Broadcast_localaddr: " << broadcast_localaddr << std::endl;
	std::cout<<"Topics file: " << topics_file << std::endl;

	LocalExchangeServer les(nh, host);
	RemoteExchangeServer res;

	std::vector<std::string> topics_list;
	std::vector<std::string> types_list;
	std::vector<int> qos_list;


	// load from topics file
	if(!topics_file.empty()) {
		std::ifstream fin(topics_file);
		json j;
		fin >> j;
		for (const auto& element : j["topics"]) {
			std::cout << element["name"] << " ( " << element["qoslvl"] << " ) " << std::endl;
			topics_list.push_back(element["name"]);
			types_list.emplace_back("");
			qos_list.emplace_back(element["qoslvl"]);
	
			les.AddPublishedTopic(element["name"], element["qoslvl"]);
		}
	}


	// forward incoming messages from local ROS service to remote services
	les.SetForward([&res](const MessageContainer &mc, int qos)
	{
		switch(qos)
		{
			case 1: res.SendMulticast(mc); break;
			case 2: res.SendMulticast(mc); break;
			case 3: res.SendBulk(mc); break;
			default:
				std::cerr << "[LES] Unhandled QOS level." << std::endl;

		}
	});

	// forward incoming messages from remote services to local ROS service
	res.SetForward([&les](const MessageContainer &mc, int qos)
	{
		les.SendToROS(mc);
	});


	// check ros master for topics
	ros::Timer timer_mastercheck = nh.createTimer(ros::Duration(5.0), [&](const ros::TimerEvent &te) 
	{
		//std::cout << "Available: " << std::endl;
		ros::master::V_TopicInfo topics;	
		ros::master::getTopics(topics);
		for(auto t : topics) {
			// std::cout << " " << t.name << " (" << t.datatype << ")" << std::endl;
			// les.UpdateTopicType(t.name, t.datatype);
			for(int i = 0; i < topics_list.size(); i++) {
				if(topics_list[i] == t.name) {
					types_list[i] = t.datatype;
					les.SubscribeToLocalRosTopic(t.name, t.datatype);
				}
			}

		}

		// print stats
		std::cout << " [STATS] " << res.GetBytesSent() << "/" << res.GetBytesRecv() << " (U/D bytes). " << res.GetMsgsSentQOS1() << " EPGM " << res.GetMsgsSentQOS2() << " MUDP " << res.GetMsgsSentQOS3() << " BTCP" << std::endl;
	});


	try 
	{
		res.Setup(broadcast_addr, broadcast_localaddr, receive_all);

		// send out service messages		
		ros::Timer timer_heartbeat = nh.createTimer(ros::Duration(3.0), [&](const ros::TimerEvent& te)  
		{

			int secs = 0, nsecs = 0;
			{
				auto time = ros::Time::now();
				secs = time.sec;
				nsecs = time.nsec;
			}

			ServiceList sl = {topics_list, types_list, qos_list, les.GetHost(), secs, nsecs, 
				res.GetBytesSent(), res.GetBytesSentQOS1(), res.GetBytesSentQOS2(), res.GetBytesSentQOS3(),
				0, 0, 0, 0};

			res.SendServiceBroadcast(sl);
		});

		// check for new messages over network
		ros::Timer timer_recv = nh.createTimer(ros::Duration(0.005), [&](const ros::TimerEvent& te) 
		{

			res.RecvServiceBroadcast([&les, self_subscribe](const std::string & topic, const std::string & type, const std::string & host, const int qoslvl) 
			{
				// subscribe to new topics
				if (self_subscribe || host != les.GetHost())
					les.PublishRemoteRosTopic(topic, type, host, qoslvl);
			}, 
			[&les, self_subscribe](const std::string & host, int qoslvl)
			{
				if (self_subscribe || host != les.GetHost())
					les.PublishRemoteReception(host, qoslvl);

			});
	
			res.RecvMulticast();

			res.RecvBulk();
		});

		// check for new messages over network
		ros::Timer timer_subcheck = nh.createTimer(ros::Duration(1.0), [&](const ros::TimerEvent& te) 
		{
			les.SubscriberCheck([&res](const std::string & host, const std::string & topic, int change, int current, int qoslvl)
			{
				const size_t topichash = std::hash<std::string>()(host + topic);
				if(change > 0 && current == 1)
					res.ChannelSubscribe(qoslvl, topichash);
				else if(change < 0 && current == 0)
					res.ChannelUnsubscribe(qoslvl, topichash);
			});

		});


		ros::spin();
	}
	catch (const zmq::error_t& e)
	{
		std::string errStr = e.what();
		std::cerr << "[ZMQ] " << errStr << std::endl;
	}	

}
