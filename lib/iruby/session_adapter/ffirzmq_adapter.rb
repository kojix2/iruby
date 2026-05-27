module IRuby
  module SessionAdapter
    class FfirzmqAdapter < BaseAdapter
      HEARTBEAT_POLL_TIMEOUT = 100

      def self.load_requirements
        require 'ffi-rzmq'
      end

      def send(sock, data)
        data.each_with_index do |part, i|
          sock.send_string(part, i == data.size - 1 ? 0 : ZMQ::SNDMORE)
        end
      end

      def recv(sock)
        recv_message(sock)
      end

      def heartbeat_loop(sock)
        poller = ZMQ::Poller.new
        poller.register_readable(sock)
        loop do
          rc = poller.poll(HEARTBEAT_POLL_TIMEOUT)
          ZMQ::Util.error_check('zmq_poll', rc)
          next unless poller.readables.include?(sock)

          msg = recv_message(sock, ZMQ::DONTWAIT)
          next if msg.empty?

          send(sock, msg)
        end
      end

      private

      def recv_message(sock, flags=0)
        msg = []
        loop do
          frame = ''
          rc = sock.recv_string(frame, flags)
          return msg if rc == -1 && ZMQ::Util.errno == ZMQ::EAGAIN

          ZMQ::Util.error_check('zmq_msg_recv', rc)
          msg << frame
          break unless sock.more_parts?
        end
        msg
      rescue
        retry if flags == 0
        msg
      end

      def make_socket(type, protocol, host, port)
        case type
        when :ROUTER, :PUB, :REP
          type = ZMQ.const_get(type)
        else
          if ZMQ.const_defined?(type)
            raise ArgumentError, "Unsupported ZMQ socket type: #{type}"
          else
            raise ArgumentError, "Invalid ZMQ socket type: #{type}"
          end
        end
        zmq_context.socket(type).tap do |sock|
          sock.bind("#{protocol}://#{host}:#{port}")
        end
      end

      def zmq_context
        @zmq_context ||= ZMQ::Context.new
      end
    end
  end
end
