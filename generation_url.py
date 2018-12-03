from random import randint

# Script from Nathan SALAUN

if __name__ == "__main__":
    pages_count = 1000
    pages_popularity = []
    pages_buffer = ""
    file_name = 'urls_%s.txt' % (pages_count)

    with open(file_name, 'a') as file:    
        for i in range(0, pages_count):
            pages_popularity.append(randint(0, 99))

        for i in range(0, pages_count):
            for j in range(0, pages_count):
                if i == j:
                    continue
                if randint(0, 99) < pages_popularity[j]:
                    file.write("https://www.example" + str(i+1) + ".org/ " + "https://www.example" + str(j+1) + ".org/" + '\n')