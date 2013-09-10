#include <cctype>
#include <cstring>
#include "esc_seq_remover.hpp"

using namespace std;

namespace Cassandra {
	const int EscapeSequencesRemover::ESCAPE = 0x1B;
	const int EscapeSequencesRemover::CSI = 0x9B;
	
	void EscapeSequencesRemover::push_character(const char c) {
		switch (_state)
		{
		case ESC_SEQ_STATE_OUTSIDE:
			if(c == ESCAPE)
				go_to_state(ESC_SEQ_STATE_AFTER_ESCAPE);
			else if(c == CSI)
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

	void EscapeSequencesRemover::go_to_state(State new_state) {
		_state = new_state;
	}

	bool EscapeSequencesRemover::is_control_character(const char c) {
		const char CONTROL_CHARACTERS[] = 
			"\x00\x0b\x0c"
			"\x0e\x0f\x18\x1a\x1b\x7f";
			
		const char* begin = CONTROL_CHARACTERS;
		const char* end = begin + sizeof(CONTROL_CHARACTERS);

		const char* it = find(begin, end, c);
		return it != end;
	}

	int EscapeSequencesRemover::read_character() {
		if(_buffer.empty())
			return -1;

		int c = _buffer.front();
		_buffer.pop_front();
		return c;
	}

	bool EscapeSequencesRemover::data_available() const {
		return _buffer.size() > 0;
	}

	string EscapeSequencesRemover::get_buffer_contents() {
		string result(_buffer.begin(), _buffer.end());
		_buffer.clear();
		return result;
	}

	bool EscapeSequencesRemover::ends_with_character(const char c) {
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

	void EscapeSequencesRemover::clear_buffer() {
		_buffer.clear();
	}
}