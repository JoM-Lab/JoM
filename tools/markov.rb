#
# Set up:
#
#   gem install sqlite3 sequel twitter
#

require 'json'
require 'sequel'
require 'twitter'

NUM_TWEETS = 10000
N_GRAM     = 2
DB_PATH    = 'data/data.db'
KWD_PATH   = 'data/keywords.json'


def train(dict, seg, n_gram)
  dict[:start] ||= []
  dict[:start] << seg[0,n_gram]

  seg.each_cons(n_gram + 1) do |a|
    dict[a[0,n_gram]] ||= []
    dict[a[0,n_gram]] << a[-1]
  end
end

def segmentize(quotes)
  quotes.map do |q|
    q.scan(/\w+\ ?|\p{Han}+|\n|[^\s]+|\s+/).map do |x|
      x.ascii_only? ? x : x.each_char.to_a
    end.flatten
  end
end

def build_dict(list_of_quotes, n_gram = 2)
  dict = {}
  # segments = segmentize(list_of_quotes.join("\n")).slice_after("\n")
  segments = segmentize(list_of_quotes.map {|a| a + "\n"})
  segments.each do |seg|
    train(dict, seg, n_gram)
  end
  dict
end

def stop_word?(word)
  /.*\n/ =~ word
end

def gen_sentence(dict, max_length = 20)
  start = dict[:start].sample
  length = start.length
  sentence = start.join

  while true
    # break if length > max_length
    new_word = dict[start].sample
    break if stop_word?(new_word)
    sentence << new_word
    start.shift; start << new_word
    length += 1
  end
  sentence
end

def load_quotes_from_db
  db = Sequel.connect("sqlite://#{DB_PATH}", readonly: true)
  tweets = db[:tweets].where(sender: 'user').order(Sequel.desc(:timestamp))
  quotes = []
  tweets.select(:text).first(NUM_TWEETS).each do |tweet|
    tweet[:text].gsub!(%r!https?://[^s]+!, '')
    next unless tweet[:text].length > 2
    quotes << tweet[:text] if match_keyword?(tweet[:text])
  end
  quotes
end

def match_keyword?(sentence)
  unless @keywords
    json ||= JSON.parse(File.read(KWD_PATH))['user']
    @keywords = json.map do |kw|
      Regexp.new(kw[0])
    end
  end

  @keywords.each do |kw|
    return true if kw =~ sentence
  end
  return false
end

def gen_wanted_sentence
  quotes = load_quotes_from_db
  dict = build_dict(quotes, N_GRAM)

  while true
    sent = gen_sentence(dict)
    return sent if sent.length > 10 and sent.length < 140
  end
end

def send_tweet
  client = Twitter::REST::Client.new do |config|
    config.consumer_key        = 'yT577ApRtZw51q4NPMPPOQ'
    config.consumer_secret     = '3neq3XqN5fO3obqwZoajavGFCUrC42ZfbrLXy5sCv8'
    config.access_token        = '3915786194-dSPebHK80hZhKNNoPfFsAtDhaECX12tyg93Hvu6'
    config.access_token_secret = 'NDeIJQHC9g6JoKP9u0TUOxDBFpbRxQcWG6IRHk8TVptII'
  end

  sent = gen_wanted_sentence
  client.update(sent)
  puts "tweeted: #{sent}"
end


def print_quote
  puts gen_wanted_sentence
end

case ARGV[0]
when "--tweet"
  send_tweet
when "--print"
  print_quote
else
  puts "usage: ruby #$0 <--tweet|--print>"
end
