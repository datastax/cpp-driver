#ifndef BOOST_IP_ADDRESS_TRAITS_
#define BOOST_IP_ADDRESS_TRAITS_

// File contains specializations of std::less<>
// and std::hash<> for boost::asio::ip:address type.

#include <boost/asio/ip/address.hpp>

namespace std {

	template<>
	struct hash<::boost::asio::ip::address> {
	public:
		typedef 
			::boost::asio::ip::address 
			argument_type;

		typedef 
			::std::size_t
			result_type;

	private:
		template<typename TBytesType>
		inline result_type
		get_hash(const TBytesType& ip_bytes) const {
			// implemented using djb2 algorithm
			// see: http://www.cse.yorku.ca/~oz/hash.html

			unsigned long hash = 5381;
			
			for(TBytesType::const_iterator it = ip_bytes.cbegin(); 
				it != ip_bytes.cend(); ++it) 
			{
				hash = ((hash << 5) + hash) + *it;
			}

			return static_cast<result_type>(hash);
		}

	public:
		inline result_type 
		operator ()(const argument_type& address) const {
			if(address.is_v4()) {
				return get_hash(address.to_v4().to_bytes());
			}
			else {
				return get_hash(address.to_v6().to_bytes());
			}
		}
	};

	template<>
	struct less<::boost::asio::ip::address> {
	public:
		typedef 
			::boost::asio::ip::address
			ip_addr;

		typedef
			ip_addr
			first_argument_type;

		typedef
			ip_addr
			second_argument_type;

		typedef
			bool
			result_type;

	private:
		typedef
			::boost::asio::ip::address_v6
			ip_v6;

		inline ip_v6
		to_ip_v6(const ip_addr& address) const {
			return address.is_v6() 
				? address.to_v6() 
				: ip_v6::v4_mapped(address.to_v4());
		}

	public:
		inline result_type
		operator ()(const ip_addr& first, const ip_addr& second) const {
			ip_v6 first_v6 = to_ip_v6(first);
			ip_v6 second_v6 = to_ip_v6(second);

			ip_v6::bytes_type first_bytes = first_v6.to_bytes();
			ip_v6::bytes_type second_bytes = second_v6.to_bytes();

			ip_v6::bytes_type::const_iterator first_it = first_bytes.cbegin();
			ip_v6::bytes_type::const_iterator second_it = second_bytes.cbegin();

			// compare addresses bytes
			for(; first_it != first_bytes.cend(); ++first_it, ++second_it) {
				int delta = (int)*first_it - (int)*second_it;
				if(delta != 0)
					return delta < 0;
			}

			// both addresses are equal
			return false;
		}
	};
}

#endif // BOOST_IP_ADDRESS_TRAITS_