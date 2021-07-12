import time, psycopg2, string, unicodedata, nltk, itertools

class ContextKeyphrasePreprocessor:

    '''
    Responsible for normalization providing essential text manipulation, and returning context effective
    feature keyphrases.
    '''

    def __init__(self, data=None):
        self.data = data
        self.punctuation = set(string.punctuation)
        # Test grammar pattern
        self.grammar = r'KT: {<DT><JJ><NN>}'
        self.chunker = nltk.chunk.regexp.RegexpParser(self.grammar)

    def generate_grammar_patterning(self, sentence=None):

        '''
        Parsing function responsible for patterning and matching the syntatic units of the parsing sentence
        with the predifined grammar by the accompanied part of speech tags.
        '''

        chunks = nltk.chunk.tree2conlltags(self.chunker.parse(sentence))
        #print(chunks)
        phrases = [
            " ".join(word for word, pos, chunk in group if word.isalpha())
            for key, group in itertools.groupby(
                chunks, lambda term: term[-1] != 'O'
            ) if key
        ]
        print("\n", phrases)
        #for phrase in phrases:
            #print(phrase)

    def normalize(self, sentence=None):

        '''
        Responsible for removing the presence of tokens of which begin with unicode category P(unctuation)
        '''

        validate_punctuation = lambda word: all(unicodedata.category(char).startswith('P') for char in word)
        sentence = filter(lambda token: not validate_punctuation(token[0]), sentence)
        sentence = map(lambda token: (token[0], token[1]), sentence)
        return list(sentence)

    def part_of_speech_generator(self):

        '''
        Function for tokenizing the content of each tweet document and returning a grammar relevant
        part of speech tag for each parsed token
        '''

        return nltk.pos_tag_sents(nltk.word_tokenize(sentence) for sentence in nltk.sent_tokenize(self.data))

    def extract_keyphrases(self):

        '''
        Responsible for integrating the generation of part of speech tags utilised in the patterning grammar
        chunk generator function.
        '''

        for sentence in self.part_of_speech_generator():
            sentence = self.normalize(sentence)
            if not sentence: continue
            self.generate_grammar_patterning(sentence)

class DataReader:

    '''
    Responsible for wrangling and reading a stream of text data extracted from database
    '''

    def __init__(self, uri=None):

        '''
        Instantiates the database connection
        '''

        self.cursor = psycopg2.connect(uri).cursor()

    def tweets(self):

        '''
        Responsible for generating tweet text corpus from database.
        '''

        self.cursor.execute("SELECT tweet_text FROM raw_tweet_table;")
        for text in list(iter(self.cursor.fetchone, None)):
            yield text

    def tweet_ids(self):

        '''
        Responsible for generating tweet_id corpus from database.
        '''

        self.cursor.execute("SELECT tweet_id FROM raw_tweet_table;")
        for tweetid in iter(self.cursor.fetchone, None):
            yield tweetid

    def transform(self):

        init = time.time()

        for tweet in self.tweets():
            print("\n\n",tweet[0])
            ContextKeyphrasePreprocessor(tweet[0]).extract_keyphrases()

        duration = time.time() - init
        print("\n\n\n", duration)
