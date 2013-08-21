#ifndef CQL_BUILDER_H_
#define CQL_BUILDER_H_


#include <list>
#include <string>
#include <boost/asio/ssl.hpp>


namespace cql {

	class cql_cluster_t;


	class cql_protocol_options_t
	{
	private:
		const std::list<std::string> _contact_points;
		const int _port;
		const boost::asio::ssl::context* _ssl_context;


	public:
		cql_protocol_options_t(const std::list<std::string>& contact_points, int port, boost::asio::ssl::context* ssl_context)
			: _contact_points(contact_points), _port(port),_ssl_context(ssl_context){}

		std::list<std::string> get_contact_points() const {return _contact_points;}
		int get_port() const{return _port;}
		const boost::asio::ssl::context* get_ssl_context() const {return _ssl_context;}
	};

	class cql_configuration_t
	{
	private:
		const cql_protocol_options_t _protocol_options;
	public:
		cql_configuration_t(const cql_protocol_options_t& protocol_options)
			: _protocol_options(protocol_options){}

		const cql_protocol_options_t* get_protocol_options() const {return &_protocol_options;}
	};

	class cql_initializer_t
	{
	public:
		virtual std::list<std::string> get_contact_points() const = 0;
		virtual cql_configuration_t* get_configuration() const = 0;
	};

    class cql_builder_t : public cql_initializer_t
	{
	private:
		std::list<std::string> _contact_points;
		int _port;
		boost::asio::ssl::context* _ssl_context;

	public:

		cql_builder_t() : _port(9042), _ssl_context(0) { }

		virtual std::list<std::string> get_contact_points() const  
		{
			return _contact_points;
		}

		int get_port() const
		{
			return _port;
		}

		cql_builder_t& with_port(int port)
		{
			_port = port;
			return *this;
		}

		cql_builder_t& with_ssl(boost::asio::ssl::context* ssl_context)
		{
			_ssl_context = ssl_context;
			return *this;
		}

		cql_builder_t& add_contact_point(std::string contact_point)
		{
			_contact_points.push_back(contact_point);
			return *this;
		}

		cql_builder_t& add_contact_points(const std::list<std::string>& contact_points)
		{
			_contact_points.insert(_contact_points.end(),contact_points.begin(),contact_points.end());
			return *this;
		}

		cql_cluster_t* build();


		virtual cql_configuration_t* get_configuration() const
		{
			return new cql_configuration_t(cql_protocol_options_t(_contact_points, _port,_ssl_context));
		}

	};

} // namespace cql

#endif // CQL_BUILDER_H_
