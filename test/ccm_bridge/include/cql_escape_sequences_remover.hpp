#ifndef CQL_ESCAPE_SEQUENCES_REMOVER_H_
#define CQL_ESCAPE_SEQUENCES_REMOVER_H_

#include <deque>
#include <string>
#include <iterator>
#include <algorithm>

namespace cql {
	// Based on: http://man7.org/linux/man-pages/man4/console_codes.4.html.
	//
	class cql_escape_sequences_remover_t {
	public:
		cql_escape_sequences_remover_t()
			: _buffer(), 
			  _state(ESC_SEQ_STATE_OUTSIDE) { }

		void push_character(const char c);

		template<typename TIterator>
		void push_character_range(TIterator begin, TIterator end);

		bool data_available() const;

		template<typename TIterator>
		size_t read(TIterator begin, TIterator end);

		int read_character();

		std::string get_buffer_contents();
		bool ends_with_character(const char c);

		void clear_buffer();
	private:
		static const int ESCAPE;
		// CSI is equivalent to ESCAPE followed by [
		static const int CSI; 

		enum State {
			// Outside any escape sequence
			ESC_SEQ_STATE_OUTSIDE,
			// ESCAPE was seen on input
			ESC_SEQ_STATE_AFTER_ESCAPE,
			// ESCAPE [ or CSI was seen on input
			ESC_SEQ_STATE_AFTER_ESCAPE_BRACKET,
			// Skip next character
			ESC_SEQ_STATE_SKIP_NEXT,
			// Skip next two characters
			ESC_SEQ_STATE_SKIP_NEXT_2,

			// Skip all characters to sequence end character
			ESC_SEQ_STATE_SKIP_TO_SEQ_END
		};
		
		void go_to_state(State new_state);
		bool is_control_character(const char c);

		std::deque<char> _buffer;
        State _state;
	};

	template<typename TIterator>
	size_t cql_escape_sequences_remover_t::read(TIterator begin, TIterator end) {
		size_t readed = 0;

		for(TIterator it = begin; it != end; ++it) {
			if(_buffer.empty())
				break;

			*it = _buffer.front();
			_buffer.pop_front();

			readed++;
		}

		return readed;
	}

	template<typename TIterator>
	void cql_escape_sequences_remover_t::push_character_range(TIterator begin, TIterator end) {
		for(TIterator it = begin; it != end; ++it)
			push_character(*it);
	}
}

#endif // CQL_ESCAPE_SEQUENCES_REMOVER_H_