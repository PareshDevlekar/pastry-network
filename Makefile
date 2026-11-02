.PHONY: all test bonus-test clean
all:
	mkdir -p ebin
	erlc -Werror -o ebin src/*.erl
test: all
	erlc -Werror -o ebin test/*.erl
	erl -noshell -pa ebin -s pastry_test run -s init stop
clean:
	rm -f ebin/*.beam

bonus-test: all
	erlc -Werror -o ebin test/failure_test.erl
	erl -noshell -pa ebin -s failure_test run -s init stop
