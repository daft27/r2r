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

	int				msg1;
	int				msg2;
	int				msg3;


	MSGPACK_DEFINE(topics, types, qoss, host, time_s, time_ns, msg1, msg2, msg3);
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

// listen to topics that need to be published elsewhere
class LocalExchangeServer
{

	struct Topic
	{
		ros::Subscriber sub;
		ros::Publisher	pub;
		std::string	type;
		int		qoslvl = 0;
	};

	ros::NodeHandle						nh_;
	std::string 						host_;
	std::map<std::string, Topic> 				topics_;
	std::map<std::string, std::map<std::string, Topic>> 	remote_topics_;
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
	void PublishRemoteRosTopic(const std::string & topic, const std::string & host)
	{
		try 
		{ 
			auto & t = remote_topics_[host][topic];
	
			if(t.pub == nullptr) {
				std::cout << " [LES] publishing " << topic << " from " << host << std::endl;
				auto pub = nh_.advertise<T>(host + topic, 5);
				t.pub = pub;
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

	void AddPublishedTopic(const std::string & topic, int qoslvl)
	{
		Topic t;
		t.qoslvl = qoslvl;
		topics_[topic] = t;
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



	void PublishRemoteRosTopic(const std::string & topic, const std::string & type, const std::string & host)
	{
		// map type strings to C++ types
		TYPE_CONVERSION(PublishRemoteRosTopic, topic, host);
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

	int				bytes_recv_ = 0;
	int				bytes_sent_ = 0;

	std::function<ForwardingCallback> forwardFunc_ = nullptr;

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
		// make publisher
		socket_ctrl_pub_ = make_unique<zmq::socket_t>(context_, ZMQ_PUB);
		{
			// see https://tools.ietf.org/html/rfc791
			int tos = 0x28 << 2; // CS5 (DSCP 101 000) or 40
			socket_ctrl_pub_->setsockopt(ZMQ_TOS, &tos, sizeof(tos));
		//	int64_t rate = 1024;
		//	socket_ctrl_pub_->setsockopt(ZMQ_RATE, &rate, sizeof(rate));
		}
		socket_ctrl_pub_->bind(broadcast_addr.c_str());
		socket_ctrl_pub_->bind(broadcast_localaddr.c_str());

		// make subscriber
		socket_ctrl_sub_ = make_unique<zmq::socket_t>(context_, ZMQ_SUB);
		socket_ctrl_sub_->setsockopt(ZMQ_SUBSCRIBE,"", 0);
		socket_ctrl_sub_->connect(broadcast_addr.c_str());
		socket_ctrl_sub_->connect(broadcast_localaddr.c_str());
	}

	void SetupMulticast()
	{
		
		socket_radio_ = make_unique<zmq::socket_t>(context_, ZMQ_RADIO);
		socket_radio_->bind("udp://239.192.1.1:6666");
		{
			// see https://tools.ietf.org/html/rfc791
			int tos = 0x2e << 2; // EF (DSCP 101 110) or 46
			socket_radio_->setsockopt(ZMQ_TOS, &tos, sizeof(tos));
		}

		socket_dish_ = make_unique<zmq::socket_t>(context_, ZMQ_DISH);
		socket_dish_->connect("udp://239.192.1.1:6666");
		zmq_join((void*) *(socket_dish_.get()), "");
	}

	void SetupBulk()
	{
		socket_bulk_pub_ = make_unique<zmq::socket_t>(context_, ZMQ_PUB);
		socket_bulk_pub_->bind("tcp://*:6667");
		{
			int limit = 2;
			socket_bulk_pub_->setsockopt(ZMQ_SNDHWM, &limit, sizeof(limit));

			uint64_t buffer = 256 * 1024;
			socket_bulk_pub_->setsockopt(ZMQ_SNDBUF, &limit, sizeof(limit));

		}

		socket_bulk_sub_ = make_unique<zmq::socket_t>(context_, ZMQ_SUB);
		socket_bulk_sub_->setsockopt(ZMQ_SUBSCRIBE,"", 0);
	}

public:

	RemoteExchangeServer() : context_(1) { }

	int GetBytesSent() const { return bytes_sent_; }
	int GetBytesRecv() const { return bytes_recv_; }

	void Setup(const std::string & broadcast_addr, const std::string & broadcast_localaddr)
	{
		SetupDiscovery(broadcast_addr, broadcast_localaddr);
		SetupMulticast();
		SetupBulk();
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
		bytes_sent_+= buffer.size();
	}


	using ServiceCallback = void(const std::string & topic, const std::string & type, const std::string & host);

	void RecvServiceBroadcast(std::function<ServiceCallback> f = nullptr)
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
					f(sl.topics[i], sl.types[i], sl.host);
				}
			}

			

			SubscribeToBulkRemote(sl.host);
			bytes_recv_+= request.size();
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
		
			bytes_recv_+= request.size();
		}
	}

	void SendBulk(const MessageContainer & data)
	{
		msgpack::sbuffer buffer;
		msgpack::pack(&buffer, data);

		zmq::message_t msg (buffer.size());
		memcpy (msg.data (), buffer.data(), buffer.size());

		if(socket_bulk_pub_->send(msg, ZMQ_NOBLOCK))
			OUT(std::cout << " [BTCP] Sent " << buffer.size() << std::endl);
		else
			std::cerr << " [BTCP] DROPPED " << buffer.size() << std::endl;

		msgs_sent_qos3_++;
		bytes_sent_+= buffer.size();
	}

	void RecvBulk()
	{
		zmq::message_t request;
		while(socket_bulk_sub_->recv (&request, ZMQ_DONTWAIT)) {
			OUT(std::cout << " [BTCP] Recv " << request.size() << std::endl);

			msgpack::unpacked result;
			msgpack::unpack(&result, (const char*) request.data(), request.size());

			msgpack::object obj = result.get();
			MessageContainer mc = obj.as<MessageContainer>();

			// mc.DebugPrint();

			Forward(mc, 2);

			bytes_recv_+= request.size();
		}
	}


	
	void SetForward(std::function<ForwardingCallback> f) { forwardFunc_ = f; }

	int GetMsgsSentQOS1() const { return msgs_sent_qos1_; }
	int GetMsgsSentQOS2() const { return msgs_sent_qos2_; }
	int GetMsgsSentQOS3() const { return msgs_sent_qos3_; }

	void Poll(std::function<ServiceCallback> f)
	{
		zmq::pollitem_t items [] = {
		        { (void*) *socket_ctrl_sub_, 0, ZMQ_POLLIN, 0 },
		        { (void*) *socket_dish_, 0, ZMQ_POLLIN, 0 },
		        { (void*) *socket_bulk_sub_, 0, ZMQ_POLLIN, 0 }
		    };

		zmq::poll (&items [0], 3, 5000);

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
		res.Setup(broadcast_addr, broadcast_localaddr);

		// send out service messages		
		ros::Timer timer_heartbeat = nh.createTimer(ros::Duration(3.0), [&](const ros::TimerEvent& te)  
		{

			int secs = 0, nsecs = 0;
			{
				auto time = ros::Time::now();
				secs = time.sec;
				nsecs = time.nsec;
			}

			ServiceList sl = {topics_list, types_list, qos_list, les.GetHost(), secs, nsecs, res.GetMsgsSentQOS1(), res.GetMsgsSentQOS2(), res.GetMsgsSentQOS3()};

			res.SendServiceBroadcast(sl);
		});

		// check for new messages over network
		ros::Timer timer_recv = nh.createTimer(ros::Duration(0.005), [&](const ros::TimerEvent& te) 
		{

			res.RecvServiceBroadcast([&les](const std::string & topic, const std::string & type, const std::string & host) 
			{
				// subscribe to new topics
				les.PublishRemoteRosTopic(topic, type, host);
			});
	
			res.RecvMulticast();

			res.RecvBulk();
		});

		ros::spin();
	}
	catch (const zmq::error_t& e)
	{
		std::string errStr = e.what();
		std::cerr << "[ZMQ] " << errStr << std::endl;
	}	

}
