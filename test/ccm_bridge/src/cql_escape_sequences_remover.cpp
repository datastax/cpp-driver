#include <cctype>
#include <cstring>
#include "cql_escape_sequences_remover.hpp"

using namespace std;

namespace cql {
	const int cql_escape_sequences_remover_t::ESCAPE = 0x1B;
	const int cql_escape_sequences_remover_t::CSI = 0x9B;
	
	void cql_escape_sequences_remover_t::push_character(const char c) {
		switch (_state)
		{
		case ESC_SEQ_STATE_OUTSIDE:
			if(c == ESCAPE)
				go_to_state(ESC_SEQ_STATE_AFTER_ESCAPE);
			else if((unsigned char)c == CSI)
				go_to_state(ESC_SEQ_STATE_AFTER_ESCAPE_BRACKET);
			else if(is_control_character(c))
				go_to_state(ESC_SEQ_STATE_OUTSIDE);
			else {
				_buffer.push_back(c);
			}
			break;

		case ESC_SEQ_STATE_AFTER_ESCAPE:
			if(c == '[')
				go_to_state(ESC_SEQ_STATE_AFTER_ESCAPE_BRACKET);
			else if(c == ']')
				go_to_state(ESC_SEQ_STATE_SKIP_TO_SEQ_END);
			else if(strchr("%#()", c) != 0)
				go_to_state(ESC_SEQ_STATE_SKIP_NEXT);
			else {
				/* current character is skipped */
				go_to_state(ESC_SEQ_STATE_OUTSIDE);
			}
			break;

		case ESC_SEQ_STATE_AFTER_ESCAPE_BRACKET:
			if(c == '[')
				go_to_state(ESC_SEQ_STATE_SKIP_NEXT);
			else
				go_to_state(ESC_SEQ_STATE_SKIP_TO_SEQ_END);
			break;

		case ESC_SEQ_STATE_SKIP_NEXT:
			/* current character is skipped */
			go_to_state(ESC_SEQ_STATE_OUTSIDE);
			break;

		case ESC_SEQ_STATE_SKIP_NEXT_2:
			/* current character is skipped */
			go_to_state(ESC_SEQ_STATE_SKIP_NEXT);
			break;

		case ESC_SEQ_STATE_SKIP_TO_SEQ_END:
			if(strchr("ABCDEFGHIJKLMPXacdefghlmnqrsu`];", c) != 0)
				go_to_state(ESC_SEQ_STATE_OUTSIDE);
			/* current character is skipped */
			break;
		}
	}

	void cql_escape_sequences_remover_t::go_to_state(State new_state) {
		_state = new_state;
	}

	bool cql_escape_sequences_remover_t::is_control_character(const char c) {
		const char CONTROL_CHARACTERS[] = 
			"\x00\x0b\x0c"
			"\x0e\x0f\x18\x1a\x1b\x7f";
			
		const char* begin = CONTROL_CHARACTERS;
		const char* end = begin + sizeof(CONTROL_CHARACTERS);

		const char* it = find(begin, end, c);
		return it != end;
	}

	int cql_escape_sequences_remover_t::read_character() {
		if(_buffer.empty())
			return -1;

		int c = _buffer.front();
		_buffer.pop_front();
		return c;
	}

	bool cql_escape_sequences_remover_t::data_available() const {
		return _buffer.size() > 0;
	}

	string cql_escape_sequences_remover_t::get_buffer_contents() {
		string result(_buffer.begin(), _buffer.end());
		_buffer.clear();
		return result;
	}

	bool cql_escape_sequences_remover_t::ends_with_character(const char c) {
		if(!data_available())
			return false;
	
		deque<char>::reverse_iterator it = _buffer.rbegin();
		for(; it != _buffer.rend(); ++it) {
			if(isspace(*it))
				continue;

			return *it == c;
		}

		return false;
	}

	void cql_escape_sequences_remover_t::clear_buffer() {
		_buffer.clear();
	}
}